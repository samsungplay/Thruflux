import { contextBridge, ipcRenderer } from "electron";
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
  showNotification: (title: string, body: string) =>
    ipcRenderer.invoke("app:showNotification", title, body),
  shareText: (title: string, text: string) =>
    ipcRenderer.invoke("app:shareText", title, text),
};

contextBridge.exposeInMainWorld("thruflux", bridge);
