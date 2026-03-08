import type { SettingsState } from "./types"

export const HEALTH_POLL_INTERVAL_MS = 1000
export const SETTINGS_STORAGE_KEY = "thruflux_settings_v1"

export const QSW_MIN = 256 * 1024
export const QSW_MAX = 2 * 1024 * 1024 * 1024
export const QCW_MIN = 1 * 1024 * 1024
export const QCW_MAX = 8 * 1024 * 1024 * 1024
export const UDP_MIN = 1 * 1024 * 1024
export const UDP_MAX = 16 * 1024 * 1024
export const MAX_RECEIVERS_MIN = 1
export const MAX_RECEIVERS_MAX = 64

export const defaultSettings: SettingsState = {
  serverUrl: "wss://bytepipe.app/ws",
  stunServer: "stun://stun.cloudflare.com:3478",
  turnServers: "",
  forceTurn: false,
  quicConnWindowBytes: 268435456,
  quicStreamWindowBytes: 33554432,
  overwrite: false,
  udpBufferBytes: 8388608,
  maxReceivers: 10,
  notifyReceiverSessionComplete: true,
  notifySenderReceiverJoined: true,
  notifySenderReceiverComplete: true,
  notifyTransferFailure: true,
}
