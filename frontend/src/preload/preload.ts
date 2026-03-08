import { contextBridge, ipcRenderer, webUtils } from "electron";
import type { ThrufluxBridge } from "../common/ipc";

const bridge: ThrufluxBridge = {
  getAppInfo: () => ipcRenderer.invoke("app:getInfo"),
  getEngineHealth: () => ipcRenderer.invoke("app:getEngineHealth"),
  getEngineEndpoint: () => ipcRenderer.invoke("app:getEngineEndpoint"),
  pickSendPaths: () => ipcRenderer.invoke("app:pickSendPaths"),
  pickSendFiles: () => ipcRenderer.invoke("app:pickSendFiles"),
  pickSendDirectories: () => ipcRenderer.invoke("app:pickSendDirectories"),
  pickReceivePath: () => ipcRenderer.invoke("app:pickReceivePath"),
  getDefaultReceiveDirectory: () =>
    ipcRenderer.invoke("app:getDefaultReceiveDirectory"),
  openPath: (targetPath: string) => ipcRenderer.invoke("app:openPath", targetPath),
  openExternal: (targetUrl: string) => ipcRenderer.invoke("app:openExternal", targetUrl),
  restartApp: () => ipcRenderer.invoke("app:restartApp"),
  showNotification: (title: string, body: string) =>
    ipcRenderer.invoke("app:showNotification", title, body),
  shareText: (title: string, text: string) =>
    ipcRenderer.invoke("app:shareText", title, text),
  resolveDroppedPath: (file: unknown) => {
    try {
      return webUtils.getPathForFile(file as File) || null;
    } catch {
      return null;
    }
  },
};

contextBridge.exposeInMainWorld("thruflux", bridge);
