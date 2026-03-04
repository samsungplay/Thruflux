import { defaultSettings, SETTINGS_STORAGE_KEY } from "./constants"
import { t } from "./strings"
import type { SendEntry, SettingsErrors, SettingsState } from "./types"

export const formatSize = (size: number): string => {
  if (size < 1024) {
    return `${size} B`
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(1)} KB`
  }
  if (size < 1024 * 1024 * 1024) {
    return `${(size / (1024 * 1024)).toFixed(1)} MB`
  }
  return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

export const splitTurnServers = (raw: string): string[] =>
  raw
    .split(/\n|,/g)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

export const toStoredTurnServers = (raw: string): string =>
  splitTurnServers(raw).join(",")

export const fromStoredTurnServers = (raw: string): string =>
  splitTurnServers(raw).join("\n")

export const validateSettings = (state: SettingsState): SettingsErrors => {
  const serverUrlOk = /^wss?:\/\//.test(state.serverUrl.trim())
  const stunOk = /^stun:\/\//.test(state.stunServer.trim())
  const turns = splitTurnServers(state.turnServers)
  const turnInvalid = turns.some((value) => !/^turn:\/\//.test(value))
  const relationOk = state.quicStreamWindowBytes <= state.quicConnWindowBytes

  return {
    serverUrl: serverUrlOk ? null : t("serverUrlError"),
    stunServer: stunOk ? null : t("stunServerError"),
    turnServers: turnInvalid ? t("turnServersError") : null,
    quicRelation: relationOk ? null : t("quicRelationError"),
  }
}

export const entriesFromDrop = (dataTransfer: DataTransfer): SendEntry[] => {
  const next: SendEntry[] = []
  const seen = new Set<string>()
  const items = Array.from(dataTransfer.items)

  for (const item of items) {
    const maybeEntry = (
      item as DataTransferItem & {
        webkitGetAsEntry?: () =>
          | { isDirectory: boolean; name?: string; fullPath?: string }
          | null
      }
    ).webkitGetAsEntry?.()

    if (maybeEntry && maybeEntry.isDirectory) {
      const path =
        maybeEntry.fullPath && maybeEntry.fullPath.length > 0
          ? maybeEntry.fullPath.replace(/^\/+/, "")
          : maybeEntry.name ?? "Folder"
      const key = `true:${path}`
      if (seen.has(key)) {
        continue
      }
      seen.add(key)
      next.push({ path, size: null, isDirectory: true })
      continue
    }

    if (item.kind === "file") {
      const file = item.getAsFile()
      if (!file) {
        continue
      }
      const path =
        file.webkitRelativePath && file.webkitRelativePath.length > 0
          ? file.webkitRelativePath
          : file.name
      const key = `false:${path}`
      if (seen.has(key)) {
        continue
      }
      seen.add(key)
      next.push({ path, size: file.size, isDirectory: false })
    }
  }

  return next
}

export const entriesFromNativePicker = (
  picked: Array<{ path: string; size: number | null; isDirectory: boolean }>,
): SendEntry[] =>
  picked.map((entry) => ({
    path: entry.path,
    size: entry.size,
    isDirectory: entry.isDirectory,
  }))

export const mergeUniqueEntries = (
  prev: SendEntry[],
  incoming: SendEntry[],
): SendEntry[] => {
  const seen = new Set(prev.map((entry) => `${entry.isDirectory}:${entry.path}`))
  const next = [...prev]
  for (const entry of incoming) {
    const key = `${entry.isDirectory}:${entry.path}`
    if (seen.has(key)) {
      continue
    }
    seen.add(key)
    next.push(entry)
  }
  return next
}

export const loadSettingsFromStorage = (): SettingsState => {
  const raw = localStorage.getItem(SETTINGS_STORAGE_KEY)
  if (!raw) {
    return { ...defaultSettings }
  }

  try {
    const parsed = JSON.parse(raw) as Partial<SettingsState>
    return {
      ...defaultSettings,
      ...parsed,
      turnServers: fromStoredTurnServers(String(parsed.turnServers ?? "")),
    }
  } catch {
    return { ...defaultSettings }
  }
}
