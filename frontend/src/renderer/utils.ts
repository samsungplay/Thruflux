import {
  defaultSettings,
  PC_JOIN_CODE_STORAGE_KEY,
  SAVED_PCS_STORAGE_KEY,
  SETTINGS_STORAGE_KEY,
} from "./constants"
import { t } from "./strings"
import type { SavedPc, SendEntry, SettingsErrors, SettingsState } from "./types"

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

export const formatThroughput = (bytesPerSecond: number): string => {
  if (bytesPerSecond < 1024) {
    return `${bytesPerSecond.toFixed(0)} B/s`
  }
  if (bytesPerSecond < 1024 * 1024) {
    return `${(bytesPerSecond / 1024).toFixed(1)} KB/s`
  }
  if (bytesPerSecond < 1024 * 1024 * 1024) {
    return `${(bytesPerSecond / (1024 * 1024)).toFixed(1)} MB/s`
  }
  return `${(bytesPerSecond / (1024 * 1024 * 1024)).toFixed(2)} GB/s`
}

export const formatEta = (
  totalBytes: number,
  movedBytes: number,
  skippedBytes: number,
  bytesPerSecond: number,
): string => {
  const done = Math.max(0, movedBytes + skippedBytes)
  const remaining = Math.max(0, totalBytes - done)
  if (remaining <= 0) {
    return "0s"
  }
  if (!Number.isFinite(bytesPerSecond) || bytesPerSecond <= 0) {
    return "Calculating..."
  }
  const seconds = Math.ceil(remaining / bytesPerSecond)
  if (seconds < 60) {
    return `${seconds}s`
  }
  const minutes = Math.floor(seconds / 60)
  const remSeconds = seconds % 60
  if (minutes < 60) {
    return remSeconds === 0 ? `${minutes}m` : `${minutes}m ${remSeconds}s`
  }
  const hours = Math.floor(minutes / 60)
  const remMinutes = minutes % 60
  return remMinutes === 0 ? `${hours}h` : `${hours}h ${remMinutes}m`
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

export const isValidJoinCode = (value: string): boolean =>
  /^[A-Za-z0-9]{16}$/.test(value.trim()) ||
  /^[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}$/.test(
    value.trim(),
  )

export const generatePcJoinCode = (): string => {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
  const values = new Uint8Array(16)
  if (globalThis.crypto) {
    globalThis.crypto.getRandomValues(values)
  } else {
    for (let idx = 0; idx < values.length; idx += 1) {
      values[idx] = Math.floor(Math.random() * 256)
    }
  }
  let suffix = ""
  for (const value of values) {
    suffix += alphabet[value % alphabet.length]
  }
  return `${suffix.slice(0, 4)}-${suffix.slice(4, 8)}-${suffix.slice(8, 12)}-${suffix.slice(12)}`
}

export const loadPcJoinCodeFromStorage = (): string => {
  const existing = localStorage.getItem(PC_JOIN_CODE_STORAGE_KEY)?.trim() ?? ""
  if (
    isValidJoinCode(existing) &&
    !existing.toUpperCase().startsWith("THRU-") &&
    existing.includes("-") &&
    existing.replace(/-/g, "").length === 16
  ) {
    return existing
  }
  const next = generatePcJoinCode()
  localStorage.setItem(PC_JOIN_CODE_STORAGE_KEY, next)
  return next
}

export const loadSavedPcsFromStorage = (): SavedPc[] => {
  const raw = localStorage.getItem(SAVED_PCS_STORAGE_KEY)
  if (!raw) {
    return []
  }
  try {
    const parsed = JSON.parse(raw) as Partial<SavedPc>[]
    if (!Array.isArray(parsed)) {
      return []
    }
    return parsed
      .filter(
        (entry): entry is SavedPc =>
          typeof entry.id === "string" &&
          typeof entry.name === "string" &&
          typeof entry.joinCode === "string" &&
          typeof entry.createdAt === "number" &&
          typeof entry.updatedAt === "number" &&
          entry.name.trim().length > 0 &&
          isValidJoinCode(entry.joinCode),
      )
      .map((entry) => ({
        ...entry,
        name: entry.name.trim(),
        joinCode: entry.joinCode.trim(),
      }))
      .sort((a, b) => b.updatedAt - a.updatedAt)
  } catch {
    return []
  }
}

export const entriesFromDrop = (dataTransfer: DataTransfer): SendEntry[] => {
  const next: SendEntry[] = []
  const seen = new Set<string>()
  const items = Array.from(dataTransfer.items)
  const droppedFiles = Array.from(dataTransfer.files) as Array<
    File & { path?: string }
  >

  for (const item of items) {
    const maybeEntry = (
      item as DataTransferItem & {
        webkitGetAsEntry?: () =>
          | { isDirectory: boolean; name?: string; fullPath?: string }
          | null
      }
    ).webkitGetAsEntry?.()

    if (maybeEntry && maybeEntry.isDirectory) {
      const dropped = item.getAsFile() as (File & { path?: string }) | null
      const resolvedPath = dropped
        ? window.thruflux.resolveDroppedPath(dropped)?.trim() ?? ""
        : ""
      const droppedPath = dropped?.path?.trim() ?? ""
      const path =
        resolvedPath.length > 0
          ? resolvedPath
          : droppedPath.length > 0
          ? droppedPath
          : maybeEntry.fullPath && maybeEntry.fullPath.length > 0
            ? maybeEntry.fullPath
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
      const resolvedPath = window.thruflux.resolveDroppedPath(file)?.trim() ?? ""
      const fileWithPath = file as File & { path?: string }
      const droppedPath = fileWithPath.path?.trim() ?? ""
      const fallbackFilePath =
        droppedFiles.find(
          (dropped) =>
            dropped.name === file.name &&
            dropped.size === file.size &&
            (dropped.path?.trim().length ?? 0) > 0,
        )?.path?.trim() ?? ""
      const path =
        resolvedPath.length > 0
          ? resolvedPath
          : droppedPath.length > 0
          ? droppedPath
          : fallbackFilePath.length > 0
            ? fallbackFilePath
          : file.webkitRelativePath && file.webkitRelativePath.length > 0
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
