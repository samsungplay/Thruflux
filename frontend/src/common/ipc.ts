export interface AppInfo {
  name: string
  version: string
  platform: NodeJS.Platform
  homepage: string | null
}

export interface EngineHealthInfo {
  alive: boolean
}

export interface EngineEndpointInfo {
  baseUrl: string | null
  port: number | null
}

export interface PickedPathEntry {
  path: string
  size: number | null
  isDirectory: boolean
}

export interface ThrufluxBridge {
  getAppInfo: () => Promise<AppInfo>
  getEngineHealth: () => Promise<EngineHealthInfo>
  getEngineEndpoint: () => Promise<EngineEndpointInfo>
  pickSendPaths: () => Promise<PickedPathEntry[]>
  pickReceivePath: () => Promise<string | null>
  getDefaultReceiveDirectory: () => Promise<string | null>
  openPath: (targetPath: string) => Promise<{ ok: boolean; error?: string }>
  openExternal: (targetUrl: string) => Promise<{ ok: boolean; error?: string }>
  showNotification: (
    title: string,
    body: string,
  ) => Promise<{ ok: boolean; error?: string }>
  shareText: (
    title: string,
    text: string,
  ) => Promise<{ ok: boolean; error?: string; method?: string }>
}
