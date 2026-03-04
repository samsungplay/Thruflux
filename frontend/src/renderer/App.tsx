import { useEffect, useMemo, useRef, useState } from "react";
import "./renderer.css";
import {
  abortReceiver,
  abort,
  setPreferredBaseUrl,
  startHost,
  startReceive,
  subscribeEvents,
  ThrufluxApiError,
} from "../common/api";
import type {
  ParsedThrufluxEvent,
  StartHostPayload,
  StartReceivePayload,
  ThrufluxEvent,
  Unsubscribe,
} from "../common/types";
import { HEALTH_POLL_INTERVAL_MS, SETTINGS_STORAGE_KEY } from "./constants";
import { AppDialog } from "./components/AppDialog";
import { HomeScreen } from "./components/HomeScreen";
import { ReceiveScreen } from "./components/ReceiveScreen";
import { SendScreen } from "./components/SendScreen";
import { SettingsScreen } from "./components/SettingsScreen";
import { TopBar } from "./components/TopBar";
import { t } from "./strings";
import type {
  AppDialogState,
  AppScreen,
  HealthState,
  ManifestProgressState,
  ReceiveFlowStage,
  ReceiveTransferProgressState,
  SendEntry,
  SendFlowStage,
  SenderTransferProgressState,
  SettingsState,
  Theme,
  ThemePreference,
} from "./types";
import {
  entriesFromDrop,
  entriesFromNativePicker,
  loadSettingsFromStorage,
  mergeUniqueEntries,
  splitTurnServers,
  toStoredTurnServers,
  validateSettings,
} from "./utils";

export default function App(): JSX.Element {
  const [themePreference, setThemePreference] = useState<ThemePreference>(
    () => {
      const savedTheme = localStorage.getItem("thruflux_theme");
      return savedTheme === "dark" || savedTheme === "light"
        ? savedTheme
        : "system";
    },
  );
  const [healthState, setHealthState] = useState<HealthState>("ongoing");
  const [hasSettledHealth, setHasSettledHealth] = useState(false);
  const [screen, setScreen] = useState<AppScreen>("home");
  const [receiveJoinCode, setReceiveJoinCode] = useState("");
  const [receiveSaveDirectory, setReceiveSaveDirectory] = useState("");
  const [isReceiveDirectoryValid, setIsReceiveDirectoryValid] = useState(false);
  const [receiveFlowStage, setReceiveFlowStage] =
    useState<ReceiveFlowStage>("idle");
  const [receiveManifestTotalSize, setReceiveManifestTotalSize] = useState(0);
  const [
    receiveManifestSummaryFilesCount,
    setReceiveManifestSummaryFilesCount,
  ] = useState(0);
  const [receiveManifestSummaryTotalSize, setReceiveManifestSummaryTotalSize] =
    useState(0);
  const [receiveTransferProgress, setReceiveTransferProgress] =
    useState<ReceiveTransferProgressState>({
      ewmaThroughput: 0,
      bytesMoved: 0,
      skippedBytes: 0,
      filesMoved: 0,
      totalExpectedFilesCount: 0,
      isRelayed: false,
      percent: 0,
    });
  const [isDropHovering, setIsDropHovering] = useState(false);
  const [sendEntries, setSendEntries] = useState<SendEntry[]>([]);
  const [sendFlowStage, setSendFlowStage] = useState<SendFlowStage>("idle");
  const [senderTransfers, setSenderTransfers] = useState<
    SenderTransferProgressState[]
  >([]);
  const [joinCode, setJoinCode] = useState("");
  const [manifestProgress, setManifestProgress] =
    useState<ManifestProgressState>({
      filesCount: 0,
      totalSize: 0,
      percent: 0,
    });
  const [isStartingSend, setIsStartingSend] = useState(false);
  const [isStartingReceive, setIsStartingReceive] = useState(false);
  const [activeTransferRole, setActiveTransferRole] = useState<
    "send" | "receive" | null
  >(null);
  const [isGoingHome, setIsGoingHome] = useState(false);
  const [dialogState, setDialogState] = useState<AppDialogState | null>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [settingsState, setSettingsState] = useState<SettingsState>(() =>
    loadSettingsFromStorage(),
  );

  const dropHoverDepthRef = useRef(0);
  const healthInFlightRef = useRef(false);
  const eventsUnsubscribeRef = useRef<Unsubscribe | null>(null);
  const toastTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeTransferRoleRef = useRef<"send" | "receive" | null>(null);
  const senderTransfersRef = useRef<SenderTransferProgressState[]>([]);
  const senderJoinedNotifiedRef = useRef<Set<string>>(new Set());
  const senderCompletedNotifiedRef = useRef<Set<string>>(new Set());
  const senderFailedNotifiedRef = useRef<Set<string>>(new Set());
  const receiverSessionCompleteNotifiedRef = useRef(false);
  const receiverSessionFailedNotifiedRef = useRef(false);
  const receiverTransferFailedRef = useRef(false);

  const theme = useMemo<Theme>(() => {
    if (themePreference !== "system") {
      return themePreference;
    }
    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }, [themePreference]);

  const errors = useMemo(
    () => validateSettings(settingsState),
    [settingsState],
  );

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
  }, [theme]);

  useEffect(() => {
    if (themePreference === "system") {
      localStorage.removeItem("thruflux_theme");
    } else {
      localStorage.setItem("thruflux_theme", themePreference);
    }
  }, [themePreference]);

  useEffect(() => {
    localStorage.setItem(
      SETTINGS_STORAGE_KEY,
      JSON.stringify({
        ...settingsState,
        turnServers: toStoredTurnServers(settingsState.turnServers),
      } satisfies SettingsState),
    );
  }, [settingsState]);

  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handleChange = (): void => {
      if (themePreference === "system") {
        document.documentElement.setAttribute(
          "data-theme",
          mq.matches ? "dark" : "light",
        );
      }
    };
    mq.addEventListener("change", handleChange);
    return () => mq.removeEventListener("change", handleChange);
  }, [themePreference]);

  useEffect(() => {
    document.title = t("appName");
    void window.thruflux.getAppInfo();
    void (async () => {
      try {
        const endpoint = await window.thruflux.getEngineEndpoint();
        if (endpoint.baseUrl) {
          setPreferredBaseUrl(endpoint.baseUrl);
        }
      } catch {}
    })();
  }, []);

  useEffect(() => {
    activeTransferRoleRef.current = activeTransferRole;
  }, [activeTransferRole]);

  useEffect(() => {
    senderTransfersRef.current = senderTransfers;
  }, [senderTransfers]);

  useEffect(() => {
    void (async () => {
      try {
        const defaultPath = await window.thruflux.getDefaultReceiveDirectory();
        if (!defaultPath) {
          return;
        }
        setReceiveSaveDirectory(defaultPath);
        setIsReceiveDirectoryValid(true);
      } catch {}
    })();
  }, []);

  const openDialog = (
    title: string,
    message: string,
    tone: AppDialogState["tone"],
  ): void => {
    setDialogState({ title, message, tone });
  };

  const showToast = (message: string): void => {
    setToastMessage(message);
    if (toastTimeoutRef.current) {
      clearTimeout(toastTimeoutRef.current);
    }
    toastTimeoutRef.current = setTimeout(() => {
      setToastMessage(null);
      toastTimeoutRef.current = null;
    }, 2600);
  };

  const isNormalClosure = (reason: string): boolean =>
    reason.trim().toLowerCase() === "normal closure";

  const readString = (value: unknown, fallback = ""): string =>
    typeof value === "string" ? value : fallback;

  const readNumber = (value: unknown, fallback = 0): number =>
    typeof value === "number" && Number.isFinite(value) ? value : fallback;

  const readBoolean = (value: unknown, fallback = false): boolean =>
    typeof value === "boolean" ? value : fallback;

  const readMessageValue = (
    message: unknown,
    keys: string[],
    fallback: unknown,
  ): unknown => {
    if (!message || typeof message !== "object") {
      return fallback;
    }
    const record = message as Record<string, unknown>;
    for (const key of keys) {
      if (key in record) {
        return record[key];
      }
    }
    return fallback;
  };

  const resetReceiveFlow = (): void => {
    setReceiveFlowStage("idle");
    setReceiveManifestTotalSize(0);
    setReceiveManifestSummaryFilesCount(0);
    setReceiveManifestSummaryTotalSize(0);
    setReceiveTransferProgress({
      ewmaThroughput: 0,
      bytesMoved: 0,
      skippedBytes: 0,
      filesMoved: 0,
      totalExpectedFilesCount: 0,
      isRelayed: false,
      percent: 0,
    });
    receiverSessionCompleteNotifiedRef.current = false;
    receiverSessionFailedNotifiedRef.current = false;
    receiverTransferFailedRef.current = false;
  };

  const sendNotification = async (title: string, body: string): Promise<void> => {
    await window.thruflux.showNotification(title, body);
  };

  const isThrufluxEvent = (
    event: ParsedThrufluxEvent,
  ): event is ThrufluxEvent => {
    switch (event.type) {
      case "connecting":
      case "connect_error":
      case "connect_success":
      case "disconnected":
      case "progress":
      case "join_code_issued":
      case "manifest_build_start":
      case "manifest_build_progress":
      case "manifest_encoding":
      case "manifest_sealed":
      case "p2p_failed":
      case "joining_session":
      case "p2p_start":
      case "p2p_success":
      case "manifest_receive_progress":
      case "manifest_receive_error":
      case "manifest_parsing":
      case "resume_notice":
      case "manifest_unsealed":
      case "quic_handshake_success":
      case "ice_not_ready":
      case "receive_complete":
        return true;
      default:
        return false;
    }
  };

  const handleSendEvent = (event: ThrufluxEvent): void => {
    switch (event.type) {
      case "manifest_build_start":
        setSendFlowStage("manifest_building");
        setManifestProgress({
          filesCount: 0,
          totalSize: 0,
          percent: 0,
        });
        return;
      case "manifest_build_progress":
        setSendFlowStage("manifest_building");
        setManifestProgress(() => ({
          filesCount: event.message.files_count,
          totalSize: event.message.total_size,
          percent: 0,
        }));
        return;
      case "manifest_encoding":
        setSendFlowStage("manifest_encoding");
        setManifestProgress((prev) => ({ ...prev, percent: 92 }));
        return;
      case "manifest_sealed":
        setSendFlowStage("manifest_sealed");
        setManifestProgress((prev) => ({
          ...prev,
          filesCount: event.message.files_count,
          totalSize: event.message.total_size,
          percent: 100,
        }));
        return;
      case "connecting":
        setSendFlowStage("connecting");
        return;
      case "connect_error":
        setSendFlowStage("idle");
        setActiveTransferRole(null);
        openDialog(
          t("connectionErrorTitle"),
          readString(readMessageValue(event.message, ["reason"], "")),
          "error",
        );
        return;
      case "connect_success":
        setSendFlowStage("connected");
        return;
      case "disconnected":
        if (
          isNormalClosure(
            readString(readMessageValue(event.message, ["reason"], "")),
          )
        ) {
          showToast(t("normalClosureToast"));
          return;
        }
        setSendFlowStage("idle");
        setJoinCode("");
        setActiveTransferRole(null);
        openDialog(
          t("disconnectedTitle"),
          readString(readMessageValue(event.message, ["reason"], "")),
          "error",
        );
        return;
      case "join_code_issued":
        setJoinCode(event.message.join_code);
        setSendFlowStage("code_ready");
        return;
      case "progress": {
        const rawReceiverId = event.message.receiverId.trim();
        const previousTransfers = senderTransfersRef.current;
        const receiverIdForEvents =
          rawReceiverId.length > 0
            ? rawReceiverId
            : `receiver-${Math.max(1, previousTransfers.length + 1)}`;
        const existing = previousTransfers.find(
          (entry) => entry.receiverId === receiverIdForEvents,
        );
        setSenderTransfers((prev) => {
          const fallbackReceiverId = `receiver-${Math.max(1, prev.length + 1)}`;
          const receiverId =
            rawReceiverId.length > 0 ? rawReceiverId : fallbackReceiverId;
          const percent = event.message.percent;
          const hasError = event.message.hasError;
          const status = hasError
            ? "failed"
            : percent >= 100
              ? "completed"
              : "ongoing";
          const nextEntry: SenderTransferProgressState = {
            receiverId,
            ewmaThroughput: event.message.ewmaThroughput,
            bytesMoved: event.message.bytesMoved,
            skippedBytes: event.message.skippedBytes,
            filesMoved: event.message.filesMoved,
            totalExpectedFilesCount: event.message.totalExpectedFilesCount,
            isRelayed: event.message.isRelayed,
            percent,
            hasError,
            status,
          };
          const idx = prev.findIndex(
            (entry) => entry.receiverId === receiverId,
          );
          if (idx === -1) {
            return [...prev, nextEntry];
          }
          const copy = [...prev];
          copy[idx] = nextEntry;
          return copy;
        });
        if (
          !existing &&
          settingsState.notifySenderReceiverJoined &&
          !senderJoinedNotifiedRef.current.has(receiverIdForEvents)
        ) {
          senderJoinedNotifiedRef.current.add(receiverIdForEvents);
          void sendNotification(
            t("notificationSenderJoinedTitle"),
            `${t("notificationSenderJoinedBody")} ${receiverIdForEvents}`,
          );
        }
        if (
          event.message.percent >= 100 &&
          !event.message.hasError &&
          settingsState.notifySenderReceiverComplete &&
          !senderCompletedNotifiedRef.current.has(receiverIdForEvents)
        ) {
          senderCompletedNotifiedRef.current.add(receiverIdForEvents);
          void sendNotification(
            t("notificationSenderCompletedTitle"),
            `${t("notificationSenderCompletedBody")} ${receiverIdForEvents}`,
          );
        }
        if (
          event.message.hasError &&
          settingsState.notifyTransferFailure &&
          !senderFailedNotifiedRef.current.has(receiverIdForEvents)
        ) {
          senderFailedNotifiedRef.current.add(receiverIdForEvents);
          void sendNotification(
            t("notificationSenderFailedTitle"),
            `${t("notificationSenderFailedBody")} ${receiverIdForEvents}`,
          );
        }

        return;
      }
      default:
        return;
    }
  };

  const handleReceiveEvent = (event: ThrufluxEvent): void => {
    switch (event.type) {
      case "connecting":
        setReceiveFlowStage("connecting");
        return;
      case "connect_error":
        resetReceiveFlow();
        setActiveTransferRole(null);
        openDialog(
          t("connectionErrorTitle"),
          readString(readMessageValue(event.message, ["reason"], "")),
          "error",
        );
        return;
      case "connect_success":
        setReceiveFlowStage("connected");
        return;
      case "disconnected":
        if (
          isNormalClosure(
            readString(readMessageValue(event.message, ["reason"], "")),
          )
        ) {
          showToast(t("normalClosureToast"));
          return;
        }
        resetReceiveFlow();
        setActiveTransferRole(null);
        openDialog(
          t("disconnectedTitle"),
          readString(readMessageValue(event.message, ["reason"], "")),
          "error",
        );
        return;
      case "p2p_failed":
        resetReceiveFlow();
        setActiveTransferRole(null);
        openDialog(
          t("receiveP2PFailedTitle"),
          t("receiveP2PStartBody"),
          "error",
        );
        return;
      case "joining_session":
        setReceiveFlowStage("joining_session");
        return;
      case "p2p_start":
        setReceiveFlowStage("p2p_start");
        return;
      case "p2p_success":
        setReceiveFlowStage("p2p_success");
        return;
      case "manifest_receive_progress":
        setReceiveFlowStage("manifest_receiving");
        setReceiveManifestTotalSize(event.message.total_size);
        return;
      case "manifest_receive_error":
        resetReceiveFlow();
        setActiveTransferRole(null);
        openDialog(
          t("receiveManifestErrorTitle"),
          `Code: ${String(event.message.errno)}`,
          "error",
        );
        return;
      case "manifest_parsing":
        setReceiveFlowStage("manifest_parsing");
        return;
      case "quic_handshake_success":
        setReceiveFlowStage("quic_ready");
        return;
      case "ice_not_ready":
        resetReceiveFlow();
        setActiveTransferRole(null);
        openDialog(
          t("receiveIceNotReadyTitle"),
          t("receiveConnectingBody"),
          "error",
        );
        return;
      case "resume_notice":
        showToast(`Continuing from ${event.message.percent.toFixed(1)}%`);
        return;
      case "manifest_unsealed":
        setReceiveManifestSummaryFilesCount(event.message.files_count);
        setReceiveManifestSummaryTotalSize(event.message.total_size);
        setReceiveFlowStage("transfer");
        setReceiveTransferProgress((prev) => ({
          ...prev,
          totalExpectedFilesCount: event.message.files_count,
          percent: 0,
        }));
        return;
      case "receive_complete":
        if (receiverTransferFailedRef.current) {
          return;
        }
        setReceiveFlowStage("complete");
        setReceiveTransferProgress((prev) => ({
          ...prev,
          percent: 100,
        }));
        if (
          settingsState.notifyReceiverSessionComplete &&
          !receiverSessionCompleteNotifiedRef.current
        ) {
          receiverSessionCompleteNotifiedRef.current = true;
          void sendNotification(
            t("notificationReceiverFinishedTitle"),
            t("notificationReceiverFinishedBody"),
          );
        }
        return;
      case "progress":
        const hasTransferError = readBoolean(
          readMessageValue(event.message, ["hasError", "has_error"], false),
          false,
        );
        const nextPercent = readNumber(
          readMessageValue(event.message, ["percent"], 0),
          0,
        );
        setReceiveTransferProgress({
          ewmaThroughput: readNumber(
            readMessageValue(
              event.message,
              ["ewmaThroughput", "ewma_throughput"],
              0,
            ),
          ),
          bytesMoved: readNumber(
            readMessageValue(event.message, ["bytesMoved", "bytes_moved"], 0),
          ),
          skippedBytes: readNumber(
            readMessageValue(
              event.message,
              ["skippedBytes", "skipped_bytes"],
              0,
            ),
          ),
          filesMoved: readNumber(
            readMessageValue(event.message, ["filesMoved", "files_moved"], 0),
          ),
          totalExpectedFilesCount: readNumber(
            readMessageValue(
              event.message,
              ["totalExpectedFilesCount", "total_expected_files_count"],
              0,
            ),
          ),
          isRelayed: readBoolean(
            readMessageValue(event.message, ["isRelayed", "is_relayed"], false),
          ),
          percent: hasTransferError ? Math.min(nextPercent, 99) : nextPercent,
        });
        if (hasTransferError) {
          receiverTransferFailedRef.current = true;
          setReceiveFlowStage("failed");
          if (
            settingsState.notifyTransferFailure &&
            !receiverSessionFailedNotifiedRef.current
          ) {
            receiverSessionFailedNotifiedRef.current = true;
            void sendNotification(
              t("notificationReceiverFailedTitle"),
              t("notificationReceiverFailedBody"),
            );
          }
        }
        return;
      default:
        return;
    }
  };

  useEffect(() => {
    let mounted = true;

    const pollHealth = async (): Promise<void> => {
      if (healthInFlightRef.current) {
        return;
      }
      healthInFlightRef.current = true;

      if (!hasSettledHealth && healthState !== "ongoing" && mounted) {
        setHealthState("ongoing");
      }

      try {
        const result = await window.thruflux.getEngineHealth();
        if (!mounted) {
          return;
        }
        setHasSettledHealth(true);
        setHealthState(result.alive ? "success" : "failed");
      } catch {
        if (!mounted) {
          return;
        }
        setHasSettledHealth(true);
        setHealthState("failed");
      } finally {
        healthInFlightRef.current = false;
      }
    };

    void pollHealth();
    const interval = window.setInterval(() => {
      void pollHealth();
    }, HEALTH_POLL_INTERVAL_MS);

    return () => {
      mounted = false;
      window.clearInterval(interval);
    };
  }, [hasSettledHealth, healthState]);

  useEffect(() => {
    if (healthState !== "success") {
      eventsUnsubscribeRef.current?.();
      eventsUnsubscribeRef.current = null;
      return;
    }

    if (eventsUnsubscribeRef.current) {
      return;
    }

    let active = true;
    void subscribeEvents((event) => {
      if (!active) {
        return;
      }
      if (!isThrufluxEvent(event)) {
        return;
      }
      if (activeTransferRoleRef.current === "receive") {
        handleReceiveEvent(event);
        return;
      }
      if (activeTransferRoleRef.current === "send") {
        handleSendEvent(event);
      }
    })
      .then((unsubscribe) => {
        if (!active) {
          unsubscribe();
          return;
        }
        eventsUnsubscribeRef.current = unsubscribe;
      })
      .catch((error: unknown) => {
        if (!active) {
          return;
        }
        const message =
          error instanceof Error
            ? error.message
            : "Could not listen for updates";
        openDialog(t("connectionErrorTitle"), message, "error");
      });

    return () => {
      active = false;
    };
  }, [healthState]);

  useEffect(() => {
    return () => {
      eventsUnsubscribeRef.current?.();
      eventsUnsubscribeRef.current = null;
      if (toastTimeoutRef.current) {
        clearTimeout(toastTimeoutRef.current);
        toastTimeoutRef.current = null;
      }
    };
  }, []);

  const openPicker = async (): Promise<void> => {
    try {
      const picked = await window.thruflux.pickSendPaths();
      if (!picked || picked.length === 0) {
        return;
      }
      const incoming = entriesFromNativePicker(picked);
      setSendEntries((prev) => mergeUniqueEntries(prev, incoming));
    } catch {}
  };

  const pickReceiveDirectory = async (): Promise<void> => {
    try {
      const selectedDirectory = await window.thruflux.pickReceivePath();
      if (!selectedDirectory) {
        return;
      }
      setReceiveSaveDirectory(selectedDirectory);
      setIsReceiveDirectoryValid(true);
    } catch {
      setIsReceiveDirectoryValid(false);
    }
  };

  const goHome = (): void => {
    void (async () => {
      if (isGoingHome) {
        return;
      }
      setIsGoingHome(true);
      try {
        const result = await abort();
        if (result.status !== 200) {
          openDialog(
            t("goHomeAbortFailedTitle"),
            `Status ${result.status}`,
            "error",
          );
          return;
        }
        setScreen("home");
        setSendFlowStage("idle");
        setSenderTransfers([]);
        senderTransfersRef.current = [];
        senderJoinedNotifiedRef.current = new Set();
        senderCompletedNotifiedRef.current = new Set();
        senderFailedNotifiedRef.current = new Set();
        setJoinCode("");
        setActiveTransferRole(null);
        setManifestProgress({
          filesCount: 0,
          totalSize: 0,
          percent: 0,
        });
        resetReceiveFlow();
        setIsDropHovering(false);
        dropHoverDepthRef.current = 0;
      } catch (error: unknown) {
        const message =
          error instanceof ThrufluxApiError
            ? error.message
            : error instanceof Error
              ? error.message
              : "Could not stop the current task";
        openDialog(t("goHomeAbortFailedTitle"), message, "error");
      } finally {
        setIsGoingHome(false);
      }
    })();
  };

  const startSending = async (): Promise<void> => {
    if (sendEntries.length === 0 || isStartingSend) {
      return;
    }

    const payload: StartHostPayload = {
      paths: sendEntries.map((entry) => entry.path),
      serverUrl: settingsState.serverUrl,
      maxReceivers: settingsState.maxReceivers,
      stunServer: settingsState.stunServer,
      turnServers: splitTurnServers(settingsState.turnServers).join(","),
      forceTurn: settingsState.forceTurn,
      quicStreamWindowBytes: settingsState.quicStreamWindowBytes,
      quicConnWindowBytes: settingsState.quicConnWindowBytes,
      udpBufferBytes: settingsState.udpBufferBytes,
    };

    setIsStartingSend(true);
    setJoinCode("");
    setSenderTransfers([]);
    senderTransfersRef.current = [];
    senderJoinedNotifiedRef.current = new Set();
    senderCompletedNotifiedRef.current = new Set();
    senderFailedNotifiedRef.current = new Set();
    setActiveTransferRole("send");
    setSendFlowStage("starting");
    setManifestProgress({
      filesCount: 0,
      totalSize: 0,
      percent: 0,
    });
    try {
      const result = await startHost(payload);
      if (result.status !== 200) {
        setSendFlowStage("idle");
        openDialog(
          t("sendStartFailedTitle"),
          `Status ${result.status}`,
          "error",
        );
      }
    } catch (error: unknown) {
      setSendFlowStage("idle");
      const message =
        error instanceof ThrufluxApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Could not start sending";
      openDialog(t("sendStartFailedTitle"), message, "error");
    } finally {
      setIsStartingSend(false);
    }
  };

  const abortSingleReceiver = async (receiverId: string): Promise<void> => {
    if (!receiverId || receiverId.trim().length === 0) {
      openDialog(
        t("senderAbortReceiverFailedTitle"),
        "Invalid receiver id",
        "error",
      );
      return;
    }
    try {
      const result = await abortReceiver({ receiverId });
      if (result.status !== 200) {
        openDialog(
          t("senderAbortReceiverFailedTitle"),
          `Status ${result.status}`,
          "error",
        );
        return;
      }
      setSenderTransfers((prev) =>
        prev.map((entry) =>
          entry.receiverId === receiverId
            ? { ...entry, hasError: true, status: "failed" }
            : entry,
        ),
      );
    } catch (error: unknown) {
      if (error instanceof ThrufluxApiError && error.status === 400) {
        openDialog(
          t("senderAbortReceiverFailedTitle"),
          error.message || "Invalid request payload",
          "error",
        );
        return;
      }
      const message =
        error instanceof ThrufluxApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Could not stop receiver";
      openDialog(t("senderAbortReceiverFailedTitle"), message, "error");
    }
  };

  const startReceiving = async (): Promise<void> => {
    if (!isReceiveDirectoryValid || isStartingReceive) {
      return;
    }
    if (receiveJoinCode.trim().length === 0) {
      openDialog(
        t("receiveStartFailedTitle"),
        t("receiveJoinCodeHint"),
        "error",
      );
      return;
    }

    const payload: StartReceivePayload = {
      joinCode: receiveJoinCode.trim(),
      out: receiveSaveDirectory,
      serverUrl: settingsState.serverUrl,
      stunServer: settingsState.stunServer,
      turnServers: splitTurnServers(settingsState.turnServers).join(","),
      forceTurn: settingsState.forceTurn,
      quicConnWindowBytes: settingsState.quicConnWindowBytes,
      quicStreamWindowBytes: settingsState.quicStreamWindowBytes,
      overwrite: settingsState.overwrite,
      udpBufferBytes: settingsState.udpBufferBytes,
    };

    setIsStartingReceive(true);
    receiverSessionCompleteNotifiedRef.current = false;
    receiverSessionFailedNotifiedRef.current = false;
    receiverTransferFailedRef.current = false;
    setActiveTransferRole("receive");
    setReceiveFlowStage("starting");
    setReceiveManifestTotalSize(0);
    setReceiveManifestSummaryFilesCount(0);
    setReceiveManifestSummaryTotalSize(0);
    setReceiveTransferProgress({
      ewmaThroughput: 0,
      bytesMoved: 0,
      skippedBytes: 0,
      filesMoved: 0,
      totalExpectedFilesCount: 0,
      isRelayed: false,
      percent: 0,
    });

    try {
      const result = await startReceive(payload);
      if (result.status !== 200) {
        resetReceiveFlow();
        setActiveTransferRole(null);
        openDialog(
          t("receiveStartFailedTitle"),
          `Status ${result.status}`,
          "error",
        );
      }
    } catch (error: unknown) {
      resetReceiveFlow();
      setActiveTransferRole(null);
      const message =
        error instanceof ThrufluxApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : t("receiveStartFailedTitle");
      openDialog(t("receiveStartFailedTitle"), message, "error");
    } finally {
      setIsStartingReceive(false);
    }
  };

  const shareJoinCode = (): void => {
    if (!joinCode) {
      return;
    }
    void (async () => {
      try {
        const shareResult = await window.thruflux.shareText(
          t("joinCodeTitle"),
          joinCode,
        );
        if (shareResult.ok) {
          if (shareResult.method === "clipboard-only") {
            openDialog(t("joinCodeTitle"), t("codeCopiedMessage"), "success");
          }
          return;
        }
      } catch {}
      try {
        if (navigator.share) {
          await navigator.share({ text: joinCode, title: t("joinCodeTitle") });
          return;
        }
      } catch {
      }
      try {
        await navigator.clipboard.writeText(joinCode);
        openDialog(t("joinCodeTitle"), t("codeCopiedMessage"), "success");
      } catch {
        openDialog(t("joinCodeTitle"), joinCode, "info");
      }
    })();
  };

  const copyJoinCode = (): void => {
    if (!joinCode) {
      return;
    }
    void (async () => {
      try {
        await navigator.clipboard.writeText(joinCode);
        openDialog(t("joinCodeTitle"), t("codeCopiedMessage"), "success");
      } catch {
        openDialog(t("joinCodeTitle"), joinCode, "info");
      }
    })();
  };

  const openReceiveFolder = (): void => {
    if (!receiveSaveDirectory) {
      return;
    }
    void (async () => {
      const result = await window.thruflux.openPath(receiveSaveDirectory);
      if (!result.ok) {
        openDialog(
          t("openFolderFailedTitle"),
          result.error ?? t("openFolderFailedTitle"),
          "error",
        );
      }
    })();
  };

  return (
    <div className="shell">
      <TopBar
        healthState={healthState}
        theme={theme}
        onSetThemePreference={setThemePreference}
      />

      {screen === "home" ? (
        <HomeScreen
          onGoSend={() => setScreen("send")}
          onGoReceive={() => setScreen("receive")}
          onGoSettings={() => setScreen("settings")}
        />
      ) : null}

      {screen === "send" ? (
        <SendScreen
          entries={sendEntries}
          isDropHovering={isDropHovering}
          onBack={goHome}
          onAbort={goHome}
          onOpenPicker={() => {
            void openPicker();
          }}
          onDragOver={(e) => {
            e.preventDefault();
          }}
          onDragEnter={(e) => {
            e.preventDefault();
            dropHoverDepthRef.current += 1;
            if (!isDropHovering) {
              setIsDropHovering(true);
            }
          }}
          onDragLeave={(e) => {
            e.preventDefault();
            dropHoverDepthRef.current = Math.max(
              0,
              dropHoverDepthRef.current - 1,
            );
            if (dropHoverDepthRef.current === 0 && isDropHovering) {
              setIsDropHovering(false);
            }
          }}
          onDrop={(e) => {
            e.preventDefault();
            dropHoverDepthRef.current = 0;
            setIsDropHovering(false);
            const dt = e.dataTransfer;
            if (!dt) {
              return;
            }
            const incoming = entriesFromDrop(dt);
            setSendEntries((prev) => mergeUniqueEntries(prev, incoming));
          }}
          onRemove={(idx) => {
            setSendEntries((prev) => prev.filter((_, i) => i !== idx));
          }}
          onConfirm={() => {
            void startSending();
          }}
          onShareJoinCode={shareJoinCode}
          onCopyJoinCode={copyJoinCode}
          onAbortReceiver={(receiverId) => {
            void abortSingleReceiver(receiverId);
          }}
          flowStage={sendFlowStage}
          joinCode={joinCode}
          isStarting={isStartingSend}
          manifestProgress={manifestProgress}
          senderTransfers={senderTransfers}
        />
      ) : null}

      {screen === "settings" ? (
        <SettingsScreen
          state={settingsState}
          errors={errors}
          onBack={goHome}
          onPatch={(patch) =>
            setSettingsState((prev) => ({
              ...prev,
              ...patch,
            }))
          }
        />
      ) : null}

      {screen === "receive" ? (
        <ReceiveScreen
          joinCode={receiveJoinCode}
          saveDirectory={receiveSaveDirectory}
          isDirectoryValid={isReceiveDirectoryValid}
          overwrite={settingsState.overwrite}
          flowStage={receiveFlowStage}
          manifestTotalSize={receiveManifestTotalSize}
          manifestSummaryFilesCount={receiveManifestSummaryFilesCount}
          manifestSummaryTotalSize={receiveManifestSummaryTotalSize}
          transferProgress={receiveTransferProgress}
          onBack={goHome}
          onJoinCodeChange={setReceiveJoinCode}
          onSelectDirectory={() => {
            void pickReceiveDirectory();
          }}
          onOverwriteChange={(value) =>
            setSettingsState((prev) => ({
              ...prev,
              overwrite: value,
            }))
          }
          onReceive={() => {
            void startReceiving();
          }}
          onAbort={goHome}
          onOpenSaveFolder={openReceiveFolder}
          onRetry={() => {
            setActiveTransferRole(null);
            resetReceiveFlow();
            setScreen("receive");
          }}
        />
      ) : null}

      {toastMessage ? (
        <div className="app-toast" role="status" aria-live="polite">
          {toastMessage}
        </div>
      ) : null}

      <AppDialog dialog={dialogState} onClose={() => setDialogState(null)} />
    </div>
  );
}
