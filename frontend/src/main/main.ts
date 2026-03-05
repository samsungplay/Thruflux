import {
  app,
  BrowserWindow,
  clipboard,
  dialog,
  ipcMain,
  Notification,
  ShareMenu,
  shell,
} from "electron";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { existsSync, promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import type {
  AppInfo,
  EngineEndpointInfo,
  EngineHealthInfo,
  PickedPathEntry,
} from "../common/ipc";

const ENGINE_START_TIMEOUT_MS = 20000;
const ENGINE_POLL_INTERVAL_MS = 400;
const ENGINE_STOP_TIMEOUT_MS = 3000;
const ENGINE_HEALTH_TIMEOUT_MS = 1200;

let engineProcess: ReturnType<typeof spawn> | null = null;
let engineStartedByApp = false;
let quittingAfterEngineStop = false;
let uiHeartbeatServer: Server | null = null;
let uiHeartbeatPort: number | null = null;
let engineApiPort: number | null = null;

const resolveWindowIconPath = (): string | undefined => {
  if (process.platform === "darwin") {
    return undefined;
  }
  const devIconPath = path.join(
    app.getAppPath(),
    "build/icons/icons/png/512x512.png",
  );
  if (existsSync(devIconPath)) {
    return devIconPath;
  }
  const packagedIconPath = path.join(
    process.resourcesPath,
    "build/icons/icons/png/512x512.png",
  );
  if (existsSync(packagedIconPath)) {
    return packagedIconPath;
  }
  return undefined;
};

const createWindow = (): void => {
  const window = new BrowserWindow({
    title: "Thruflux",
    icon: resolveWindowIconPath(),
    width: 1200,
    height: 800,
    minWidth: 960,
    minHeight: 640,
    webPreferences: {
      preload: path.join(__dirname, "../preload/preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  window.loadFile(path.join(__dirname, "../renderer/index.html"));
  // window.webContents.openDevTools();
};

const registerIpc = (): void => {
  ipcMain.handle("app:getInfo", () => {
    const info: AppInfo = {
      name: app.getName(),
      version: app.getVersion(),
      platform: process.platform,
    };
    return info;
  });

  ipcMain.handle("app:getEngineHealth", async () => {
    const result = await checkEngineHealth();
    const info: EngineHealthInfo = {
      alive: result.alive,
    };
    return info;
  });

  ipcMain.handle("app:getEngineEndpoint", () => {
    const info: EngineEndpointInfo = {
      baseUrl:
        typeof engineApiPort === "number"
          ? `http://127.0.0.1:${engineApiPort}`
          : null,
      port: engineApiPort,
    };
    return info;
  });

  ipcMain.handle("app:pickSendPaths", async () => {
    const window =
      BrowserWindow.getFocusedWindow() ?? BrowserWindow.getAllWindows()[0];
    const result = await dialog.showOpenDialog(window, {
      properties: ["openFile", "openDirectory", "multiSelections"],
    });
    if (result.canceled || result.filePaths.length === 0) {
      return [] as PickedPathEntry[];
    }

    const entries: PickedPathEntry[] = [];
    for (const selectedPath of result.filePaths) {
      try {
        const stats = await fs.stat(selectedPath);
        entries.push({
          path: selectedPath,
          isDirectory: stats.isDirectory(),
          size: stats.isDirectory() ? null : stats.size,
        });
      } catch {}
    }
    return entries;
  });

  ipcMain.handle("app:pickReceivePath", async () => {
    const window =
      BrowserWindow.getFocusedWindow() ?? BrowserWindow.getAllWindows()[0];
    const result = await dialog.showOpenDialog(window, {
      properties: ["openDirectory"],
    });
    if (result.canceled || result.filePaths.length === 0) {
      return null as string | null;
    }
    return result.filePaths[0];
  });

  ipcMain.handle("app:getDefaultReceiveDirectory", async () => {
    const downloadsDir = path.join(os.homedir(), "Downloads");
    try {
      const stats = await fs.stat(downloadsDir);
      if (stats.isDirectory()) {
        return downloadsDir;
      }
      return null as string | null;
    } catch {
      return null as string | null;
    }
  });

  ipcMain.handle("app:openPath", async (_event, targetPath: string) => {
    if (!targetPath || typeof targetPath !== "string") {
      return { ok: false, error: "Invalid path" };
    }
    try {
      const result = await shell.openPath(targetPath);
      if (result && result.length > 0) {
        return { ok: false, error: result };
      }
      return { ok: true };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error.message : "Failed to open path",
      };
    }
  });

  ipcMain.handle(
    "app:showNotification",
    (_event, title: string, body: string) => {
      if (
        typeof title !== "string" ||
        title.trim().length === 0 ||
        typeof body !== "string"
      ) {
        return { ok: false, error: "Invalid notification payload" };
      }
      if (!Notification.isSupported()) {
        return { ok: false, error: "Notifications are not supported" };
      }
      const notification = new Notification({
        title: title.trim(),
        body,
      });
      notification.on("click", () => {
        const window =
          BrowserWindow.getFocusedWindow() ?? BrowserWindow.getAllWindows()[0];
        if (!window) {
          return;
        }
        if (window.isMinimized()) {
          window.restore();
        }
        if (!window.isVisible()) {
          window.show();
        }
        window.focus();
      });
      notification.show();
      return { ok: true };
    },
  );

  ipcMain.handle("app:shareText", async (_event, title: string, text: string) => {
    if (
      typeof title !== "string" ||
      title.trim().length === 0 ||
      typeof text !== "string" ||
      text.trim().length === 0
    ) {
      return { ok: false, error: "Invalid share payload" };
    }

    const window =
      BrowserWindow.getFocusedWindow() ?? BrowserWindow.getAllWindows()[0];

    if (process.platform === "darwin" && window) {
      try {
        const menu = new ShareMenu({
          texts: [text],
        });
        menu.popup({
          window,
        });
        return { ok: true, method: "share-menu" };
      } catch {}
    }

    try {
      clipboard.writeText(text);
    } catch {}

    try {
      const params = new URLSearchParams({
        subject: title.trim(),
        body: text,
      });
      await shell.openExternal(`mailto:?${params.toString()}`);
      return { ok: true, method: "clipboard-mailto" };
    } catch {
      return { ok: true, method: "clipboard-only" };
    }
  });
};

const sleep = async (ms: number): Promise<void> => {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
};

const getEnginePlatformDir = (): "windows" | "linux" | "macos" => {
  if (process.platform === "win32") {
    return "windows";
  }
  if (process.platform === "darwin") {
    return "macos";
  }
  return "linux";
};

const getEngineBinaryName = (): string => {
  if (process.platform === "win32") {
    return "thru_windows.exe";
  }
  if (process.platform === "darwin") {
    return "thru_mac";
  }
  return "thru_linux";
};

const buildEngineArgs = (
  heartbeatPort: number,
  enginePort: number,
): string[] => {
  return [
    "ui",
    "--port",
    String(enginePort),
    "--ui-heartbeat-port",
    String(heartbeatPort),
  ];
};

const reserveFreePort = async (): Promise<number> => {
  const server = net.createServer();
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "::", () => {
        server.off("error", reject);
        resolve();
      });
    });
  } catch {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        server.off("error", reject);
        resolve();
      });
    });
  }
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Failed to reserve a free engine port");
  }
  const port = address.port;
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
  return port;
};

const startUiHeartbeatServer = async (): Promise<number> => {
  if (uiHeartbeatServer && typeof uiHeartbeatPort === "number") {
    return uiHeartbeatPort;
  }

  const server = createServer((req, res) => {
    if (
      req.url === "/health" &&
      (req.method === "GET" || req.method === "HEAD")
    ) {
      res.statusCode = 200;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      res.end("ok");
      return;
    }
    res.statusCode = 404;
    res.end();
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Failed to determine heartbeat server address");
  }
  uiHeartbeatServer = server;
  uiHeartbeatPort = address.port;
  return address.port;
};

const stopUiHeartbeatServer = async (): Promise<void> => {
  if (!uiHeartbeatServer) {
    return;
  }
  const server = uiHeartbeatServer;
  uiHeartbeatServer = null;
  uiHeartbeatPort = null;
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
};

const fetchHealth = async (baseUrl: string): Promise<boolean> => {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    ENGINE_HEALTH_TIMEOUT_MS,
  );
  try {
    const response = await fetch(`${baseUrl}/health`, {
      method: "GET",
      signal: controller.signal,
    });

    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
};

const checkEngineHealth = async (): Promise<{ alive: boolean }> => {
  if (typeof engineApiPort !== "number") {
    return { alive: false };
  }
  const baseUrl = `http://127.0.0.1:${engineApiPort}`;

  const alive = await fetchHealth(baseUrl);
  return { alive };
};

const resolveEngineBinaryPath = async (): Promise<string> => {
  const platformDir = getEnginePlatformDir();
  const binaryName = getEngineBinaryName();
  const appPath = app.getAppPath();
  const candidates = [
    path.join(process.resourcesPath, "binaries", platformDir, binaryName),
    path.join(appPath, "binaries", platformDir, binaryName),
    path.join(process.cwd(), "binaries", platformDir, binaryName),
  ];

  for (const candidate of candidates) {
    try {
      await fs.access(candidate, fs.constants.F_OK);
      return candidate;
    } catch {}
  }

  throw new Error(
    `Engine binary not found for ${process.platform}. Checked: ${candidates.join(", ")}`,
  );
};

const waitForEngineHealth = async (timeoutMs: number): Promise<boolean> => {
  const startedAt = Date.now();
  let attempt = 0;
  while (Date.now() - startedAt < timeoutMs) {
    attempt += 1;
    const status = await checkEngineHealth();
    if (status.alive) {
      console.log(
        `[engine] health ready on http://127.0.0.1:${String(engineApiPort)} after ${attempt} checks`,
      );
      return true;
    }
    if (attempt === 1 || attempt % 5 === 0) {
      console.log(
        `[engine] waiting for health on http://127.0.0.1:${String(engineApiPort)} (attempt ${attempt})`,
      );
    }
    await sleep(ENGINE_POLL_INTERVAL_MS);
  }
  console.error(
    `[engine] health timeout after ${attempt} checks on http://127.0.0.1:${String(engineApiPort)}`,
  );
  return false;
};

const stopManagedEngine = async (): Promise<void> => {
  if (!engineStartedByApp || !engineProcess) {
    return;
  }

  const child = engineProcess;
  const alreadyStopped = child.exitCode !== null || child.killed;

  if (!alreadyStopped) {
    child.kill();
    await Promise.race([once(child, "exit"), sleep(ENGINE_STOP_TIMEOUT_MS)]);

    if (child.exitCode === null) {
      child.kill("SIGKILL");
      await Promise.race([once(child, "exit"), sleep(500)]);
    }
  }

  engineProcess = null;
  engineStartedByApp = false;
  engineApiPort = null;
};

const ensureEngineReady = async (): Promise<void> => {
  if (typeof uiHeartbeatPort !== "number") {
    throw new Error("Heartbeat server is not ready");
  }
  if (engineProcess && engineProcess.exitCode === null) {
    return;
  }
  engineApiPort = await reserveFreePort();

  const engineBinaryPath = await resolveEngineBinaryPath();
  const engineArgs = buildEngineArgs(uiHeartbeatPort, engineApiPort);
  console.log(
    `[engine] spawning binary=${engineBinaryPath} args=${engineArgs.join(" ")}`,
  );

  const child = spawn(engineBinaryPath, engineArgs, {
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  child.once("spawn", () => {
    console.log(`[engine] spawned pid=${String(child.pid)}`);
  });
  child.stdout?.on("data", (d) =>
    console.log(`[engine] ${d.toString().trimEnd()}`),
  );
  child.stderr?.on("data", (d) =>
    console.error(`[engine] ${d.toString().trimEnd()}`),
  );

  engineProcess = child;
  engineStartedByApp = true;

  child.once("error", (err) => {
    console.error("[engine] spawn error", err);
  });

  child.once("exit", (code, signal) => {
    console.log("[engine] exited", { code, signal });

    if (engineProcess && engineProcess.pid === child.pid) {
      engineProcess = null;
      engineApiPort = null;
    }
  });

  const ready = await waitForEngineHealth(ENGINE_START_TIMEOUT_MS);
  if (!ready) {
    await stopManagedEngine();
    throw new Error("Engine failed to become healthy within startup timeout");
  }
};

app.whenReady().then(() => {
  registerIpc();
  void (async () => {
    try {
      console.log("[engine] app ready, initializing engine bootstrap");
      await startUiHeartbeatServer();
      console.log(
        `[engine] ui heartbeat server listening on 127.0.0.1:${String(uiHeartbeatPort)}`,
      );
      createWindow();

      await ensureEngineReady();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`Engine startup failed: ${message}`);
      if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
      }
    }
  })();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("before-quit", (event) => {
  if (quittingAfterEngineStop || !engineStartedByApp || !engineProcess) {
    void stopUiHeartbeatServer();
    return;
  }

  event.preventDefault();
  void (async () => {
    await stopManagedEngine();
    await stopUiHeartbeatServer();
    quittingAfterEngineStop = true;
    app.exit(0);
  })();
});

app.on("window-all-closed", () => {
  // if (process.platform !== "darwin") {
  app.quit();
  // }
});
