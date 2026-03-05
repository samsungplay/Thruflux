export type Theme = "light" | "dark"
export type ThemePreference = Theme | "system"
export type HealthState = "success" | "ongoing" | "failed"
export type AppScreen = "home" | "send" | "receive" | "settings"
export type SendFlowStage =
  | "idle"
  | "starting"
  | "manifest_building"
  | "manifest_encoding"
  | "manifest_sealed"
  | "connecting"
  | "connected"
  | "code_ready"
export type ReceiveFlowStage =
  | "idle"
  | "starting"
  | "connecting"
  | "connected"
  | "joining_session"
  | "p2p_start"
  | "p2p_success"
  | "manifest_receiving"
  | "manifest_parsing"
  | "quic_ready"
  | "transfer"
  | "failed"
  | "complete"
export type DialogTone = "error" | "success" | "info"

export type Locale = "en"

export type TranslationKey =
  | "appName"
  | "appSlogan"
  | "statusLabel"
  | "statusReady"
  | "statusChecking"
  | "statusNotReady"
  | "themeLight"
  | "themeDark"
  | "sendTitle"
  | "sendBody"
  | "receiveTitle"
  | "receiveBody"
  | "sendScreenTitle"
  | "sendScreenBody"
  | "sendScreenHint"
  | "sendSummaryFiles"
  | "sendSummaryFolders"
  | "sendDropMoreHint"
  | "sendDropOverlay"
  | "sendSizeUnknown"
  | "sendConfirm"
  | "receiveScreenTitle"
  | "receiveScreenBody"
  | "receiveJoinCodeLabel"
  | "receiveJoinCodePlaceholder"
  | "receiveJoinCodeHint"
  | "receiveSaveDirectoryLabel"
  | "receiveSaveDirectoryHint"
  | "receiveSaveDirectoryButton"
  | "receiveActionButton"
  | "receiveOverwriteBody"
  | "receiveStartingTitle"
  | "receiveStartingBody"
  | "receiveConnectingTitle"
  | "receiveConnectingBody"
  | "receiveConnectedTitle"
  | "receiveConnectedBody"
  | "receiveJoiningTitle"
  | "receiveJoiningBody"
  | "receiveP2PStartTitle"
  | "receiveP2PStartBody"
  | "receiveP2PSuccessTitle"
  | "receiveP2PSuccessBody"
  | "receiveManifestTitle"
  | "receiveManifestBody"
  | "receiveManifestSizeLabel"
  | "receiveManifestParsingTitle"
  | "receiveManifestParsingBody"
  | "receiveSecureLinkTitle"
  | "receiveSecureLinkBody"
  | "receiveTransferTitle"
  | "receiveTransferBody"
  | "receiveExpectedFilesLabel"
  | "receiveExpectedSizeLabel"
  | "receiveThroughputLabel"
  | "receiveMovedLabel"
  | "receiveSkippedLabel"
  | "receiveFilesDoneLabel"
  | "receiveRouteLabel"
  | "receivePercentLabel"
  | "receiveRouteDirect"
  | "receiveRouteRelayed"
  | "receiveStartFailedTitle"
  | "receiveP2PFailedTitle"
  | "receiveManifestErrorTitle"
  | "receiveIceNotReadyTitle"
  | "receiveTransferFailedTitle"
  | "receiveTransferFailedBody"
  | "receiveCompleteTitle"
  | "receiveCompleteBody"
  | "receiveFailedBadge"
  | "normalClosureToast"
  | "backHome"
  | "settingsTitle"
  | "settingsWarning"
  | "settingsPanelTitle"
  | "settingsPanelBody"
  | "appVersionNote"
  | "appMadeBy"
  | "updateAvailableTitle"
  | "updateAvailableBody"
  | "updateNow"
  | "openSettings"
  | "serverUrl"
  | "stunServer"
  | "turnServers"
  | "forceTurn"
  | "quicConnWindowBytes"
  | "quicStreamWindowBytes"
  | "overwrite"
  | "udpBufferBytes"
  | "maxReceivers"
  | "notificationSettingsTitle"
  | "notificationReceiverFinishedLabel"
  | "notificationReceiverFinishedHint"
  | "notificationSenderJoinedLabel"
  | "notificationSenderJoinedHint"
  | "notificationSenderCompletedLabel"
  | "notificationSenderCompletedHint"
  | "notificationReceiverFinishedTitle"
  | "notificationReceiverFinishedBody"
  | "notificationSenderJoinedTitle"
  | "notificationSenderJoinedBody"
  | "notificationSenderCompletedTitle"
  | "notificationSenderCompletedBody"
  | "notificationTransferFailedLabel"
  | "notificationTransferFailedHint"
  | "notificationReceiverFailedTitle"
  | "notificationReceiverFailedBody"
  | "notificationSenderFailedTitle"
  | "notificationSenderFailedBody"
  | "serverUrlError"
  | "serverUrlHint"
  | "stunServerError"
  | "stunServerHint"
  | "turnServersError"
  | "quicRelationError"
  | "turnServersHint"
  | "forceTurnHint"
  | "qcwHint"
  | "qswHint"
  | "udpHint"
  | "maxReceiversHint"
  | "overwriteHint"
  | "autoResumeHint"
  | "sendPreparingTitle"
  | "sendPreparingBody"
  | "sendConnectingTitle"
  | "sendConnectingBody"
  | "sendConnectedTitle"
  | "sendConnectedBody"
  | "manifestBuildTitle"
  | "manifestBuildBody"
  | "manifestEncodingTitle"
  | "manifestEncodingBody"
  | "manifestSealedTitle"
  | "manifestSealedBody"
  | "manifestFilesCountLabel"
  | "manifestTotalSizeLabel"
  | "joinCodeTitle"
  | "joinCodeBody"
  | "shareCode"
  | "copyCode"
  | "codeCopiedMessage"
  | "receiverListTitle"
  | "receiverWaiting"
  | "sendStartFailedTitle"
  | "connectionErrorTitle"
  | "disconnectedTitle"
  | "goHomeAbortFailedTitle"
  | "dismiss"
  | "sendNow"
  | "senderNoReceiversTitle"
  | "senderNoReceiversBody"
  | "senderTransfersTitle"
  | "senderReceiverLabel"
  | "senderStatusOngoing"
  | "senderStatusCompleted"
  | "senderStatusFailed"
  | "senderAbortReceiverButton"
  | "senderAbortReceiverFailedTitle"
  | "sendAbortButton"
  | "receiveAbortButton"
  | "receiveOpenFolderButton"
  | "receiveRetryButton"
  | "openFolderFailedTitle"

export interface SendEntry {
  path: string
  size: number | null
  isDirectory: boolean
}

export interface SettingsState {
  serverUrl: string
  stunServer: string
  turnServers: string
  forceTurn: boolean
  quicConnWindowBytes: number
  quicStreamWindowBytes: number
  overwrite: boolean
  udpBufferBytes: number
  maxReceivers: number
  notifyReceiverSessionComplete: boolean
  notifySenderReceiverJoined: boolean
  notifySenderReceiverComplete: boolean
  notifyTransferFailure: boolean
}

export interface SettingsErrors {
  serverUrl: string | null
  stunServer: string | null
  turnServers: string | null
  quicRelation: string | null
}

export interface AppDialogState {
  title: string
  message: string
  tone: DialogTone
  actionLabel?: string
  onAction?: () => void
}

export interface ManifestProgressState {
  filesCount: number
  totalSize: number
  percent: number
}

export interface ReceiveTransferProgressState {
  ewmaThroughput: number
  bytesMoved: number
  skippedBytes: number
  filesMoved: number
  totalExpectedFilesCount: number
  isRelayed: boolean
  percent: number
}

export type TransferBadgeStatus = "ongoing" | "completed" | "failed"

export interface SenderTransferProgressState extends ReceiveTransferProgressState {
  receiverId: string
  hasError: boolean
  status: TransferBadgeStatus
}
