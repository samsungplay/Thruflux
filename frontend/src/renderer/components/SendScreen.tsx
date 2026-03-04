import type { DragEvent, KeyboardEvent } from "react"
import { t } from "../strings"
import { formatSize } from "../utils"
import type {
  ManifestProgressState,
  SendEntry,
  SendFlowStage,
  SenderTransferProgressState,
} from "../types"

interface SendScreenProps {
  entries: SendEntry[]
  isDropHovering: boolean
  onBack: () => void
  onAbort: () => void
  onOpenPicker: () => void
  onDrop: (e: DragEvent<HTMLDivElement>) => void
  onDragEnter: (e: DragEvent<HTMLDivElement>) => void
  onDragLeave: (e: DragEvent<HTMLDivElement>) => void
  onDragOver: (e: DragEvent<HTMLDivElement>) => void
  onRemove: (idx: number) => void
  onConfirm: () => void
  onShareJoinCode: () => void
  onCopyJoinCode: () => void
  onAbortReceiver: (receiverId: string) => void
  flowStage: SendFlowStage
  joinCode: string
  isStarting: boolean
  manifestProgress: ManifestProgressState
  senderTransfers: SenderTransferProgressState[]
}

export function SendScreen({
  entries,
  isDropHovering,
  onBack,
  onAbort,
  onOpenPicker,
  onDrop,
  onDragEnter,
  onDragLeave,
  onDragOver,
  onRemove,
  onConfirm,
  onShareJoinCode,
  onCopyJoinCode,
  onAbortReceiver,
  flowStage,
  joinCode,
  isStarting,
  manifestProgress,
  senderTransfers,
}: SendScreenProps): JSX.Element {
  const fileCount = entries.filter((entry) => !entry.isDirectory).length
  const folderCount = entries.filter((entry) => entry.isDirectory).length

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault()
      onOpenPicker()
    }
  }

  if (flowStage !== "idle") {
    return (
      <main className="send-shell state-mode">
        <div className="send-state-actions">
          <button className="back-btn" type="button" onClick={onBack}>
            <i className="fa-solid fa-house"></i>
            <span>{t("backHome")}</span>
          </button>
          <button className="send-abort-btn" type="button" onClick={onAbort}>
            <i className="fa-solid fa-stop"></i>
            <span>{t("sendAbortButton")}</span>
          </button>
        </div>
        <section
          className={`send-state-card ${
            flowStage === "code_ready" && senderTransfers.length > 0
              ? "sender-transfer-mode"
              : ""
          }`}
        >
          {flowStage === "starting" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-gear"></i>
              </span>
              <h1>{t("sendPreparingTitle")}</h1>
              <p>{t("sendPreparingBody")}</p>
            </>
          ) : null}
          {flowStage === "manifest_building" ? (
            <div className="manifest-shell">
              <span className="state-icon spin">
                <i className="fa-solid fa-folder-tree"></i>
              </span>
              <h1>{t("manifestBuildTitle")}</h1>
              <p>{t("manifestBuildBody")}</p>
              <div className="manifest-progress-track">
                <div className="manifest-progress-fill indeterminate"></div>
              </div>
              <div className="manifest-metrics">
                <span>
                  {t("manifestFilesCountLabel")}: {manifestProgress.filesCount}
                </span>
                <span>
                  {t("manifestTotalSizeLabel")}: {formatSize(manifestProgress.totalSize)}
                </span>
              </div>
            </div>
          ) : null}
          {flowStage === "manifest_encoding" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-box-archive"></i>
              </span>
              <h1>{t("manifestEncodingTitle")}</h1>
              <p>{t("manifestEncodingBody")}</p>
              <div className="manifest-progress-track">
                <div className="manifest-progress-fill encoding"></div>
              </div>
            </>
          ) : null}
          {flowStage === "manifest_sealed" ? (
            <>
              <span className="state-icon success">
                <i className="fa-solid fa-circle-check"></i>
              </span>
              <h1>{t("manifestSealedTitle")}</h1>
              <p>{t("manifestSealedBody")}</p>
              <div className="manifest-progress-track">
                <div className="manifest-progress-fill" style={{ width: "100%" }}></div>
              </div>
            </>
          ) : null}
          {flowStage === "connecting" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-cloud"></i>
              </span>
              <h1>{t("sendConnectingTitle")}</h1>
              <p>{t("sendConnectingBody")}</p>
            </>
          ) : null}
          {flowStage === "connected" ? (
            <>
              <span className="state-icon success">
                <i className="fa-solid fa-circle-check"></i>
              </span>
              <h1>{t("sendConnectedTitle")}</h1>
              <p>{t("sendConnectedBody")}</p>
            </>
          ) : null}
          {flowStage === "code_ready" ? (
            senderTransfers.length === 0 ? (
              <div className="join-code-shell">
                <span className="state-icon success">
                  <i className="fa-solid fa-key"></i>
                </span>
                <h1>{t("joinCodeTitle")}</h1>
                <p>{t("senderNoReceiversBody")}</p>
                <div className="join-code-box">{joinCode}</div>
                <div className="manifest-final-summary">
                  <span>
                    {t("manifestFilesCountLabel")}: {manifestProgress.filesCount}
                  </span>
                  <span>
                    {t("manifestTotalSizeLabel")}: {formatSize(manifestProgress.totalSize)}
                  </span>
                </div>
                <div className="join-code-actions">
                  <button className="share-code-btn" type="button" onClick={onShareJoinCode}>
                    <i className="fa-solid fa-share-nodes"></i>
                    <span>{t("shareCode")}</span>
                  </button>
                  <button className="copy-code-btn" type="button" onClick={onCopyJoinCode}>
                    <i className="fa-solid fa-copy"></i>
                    <span>{t("copyCode")}</span>
                  </button>
                </div>
              </div>
            ) : (
              <div className="sender-transfer-shell">
                <section className="sender-transfer-top">
                  <div className="sender-transfer-top-left">
                    <h1>{t("joinCodeTitle")}</h1>
                    <p>{t("senderTransfersTitle")}</p>
                  </div>
                  <div className="sender-transfer-top-right">
                    <div className="join-code-box compact">{joinCode}</div>
                    <div className="join-code-actions">
                      <button className="share-code-btn" type="button" onClick={onShareJoinCode}>
                        <i className="fa-solid fa-share-nodes"></i>
                        <span>{t("shareCode")}</span>
                      </button>
                      <button className="copy-code-btn" type="button" onClick={onCopyJoinCode}>
                        <i className="fa-solid fa-copy"></i>
                        <span>{t("copyCode")}</span>
                      </button>
                    </div>
                  </div>
                </section>
                <div className="manifest-final-summary">
                  <span>
                    {t("manifestFilesCountLabel")}: {manifestProgress.filesCount}
                  </span>
                  <span>
                    {t("manifestTotalSizeLabel")}: {formatSize(manifestProgress.totalSize)}
                  </span>
                </div>
                <section className="sender-transfer-list-wrap">
                  <ul className="sender-transfer-list">
                    {senderTransfers.map((entry) => (
                      <li className="sender-transfer-item" key={entry.receiverId}>
                        <div className="sender-transfer-item-head">
                          <span>
                            {t("senderReceiverLabel")}: {entry.receiverId}
                          </span>
                          <div className="sender-transfer-item-actions">
                            <span className={`sender-transfer-badge ${entry.status}`}>
                              {entry.status === "completed"
                                ? t("senderStatusCompleted")
                                : entry.status === "failed"
                                  ? t("senderStatusFailed")
                                  : t("senderStatusOngoing")}
                            </span>
                            {entry.status === "ongoing" ? (
                              <button
                                className="sender-receiver-abort-btn"
                                type="button"
                                onClick={() => onAbortReceiver(entry.receiverId)}
                              >
                                <i className="fa-solid fa-stop"></i>
                                <span>{t("senderAbortReceiverButton")}</span>
                              </button>
                            ) : null}
                          </div>
                        </div>
                        <div className="transfer-progress-head">
                          <span>{t("receivePercentLabel")}</span>
                          <strong>{Math.round(entry.percent)}%</strong>
                        </div>
                        <div className="manifest-progress-track sender-progress-track">
                          <div
                            className="manifest-progress-fill"
                            style={{ width: `${Math.max(0, Math.min(100, entry.percent))}%` }}
                          ></div>
                        </div>
                        <div className="receive-transfer-metrics">
                          <div>
                            <span>{t("receiveThroughputLabel")}</span>
                            <strong>
                              {entry.ewmaThroughput < 1024
                                ? `${entry.ewmaThroughput.toFixed(0)} B/s`
                                : entry.ewmaThroughput < 1024 * 1024
                                  ? `${(entry.ewmaThroughput / 1024).toFixed(1)} KB/s`
                                  : `${(entry.ewmaThroughput / (1024 * 1024)).toFixed(1)} MB/s`}
                            </strong>
                          </div>
                          <div>
                            <span>{t("receiveMovedLabel")}</span>
                            <strong>{formatSize(entry.bytesMoved)}</strong>
                          </div>
                          <div>
                            <span>{t("receiveSkippedLabel")}</span>
                            <strong>{formatSize(entry.skippedBytes)}</strong>
                          </div>
                          <div>
                            <span>{t("receiveFilesDoneLabel")}</span>
                            <strong>
                              {entry.filesMoved}/{entry.totalExpectedFilesCount}
                            </strong>
                          </div>
                          <div>
                            <span>{t("receiveRouteLabel")}</span>
                            <strong>
                              {entry.isRelayed
                                ? t("receiveRouteRelayed")
                                : t("receiveRouteDirect")}
                            </strong>
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </section>
              </div>
            )
          ) : null}
        </section>
      </main>
    )
  }

  return (
    <main className="send-shell">
      <button className="back-btn" type="button" onClick={onBack}>
        <i className="fa-solid fa-house"></i>
        <span>{t("backHome")}</span>
      </button>

      <section className="send-summary">
        <div className="send-summary-pill">
          <i className="fa-solid fa-file" aria-hidden="true"></i>
          <span>{fileCount} {t("sendSummaryFiles")}</span>
        </div>
        <div className="send-summary-pill">
          <i className="fa-solid fa-folder" aria-hidden="true"></i>
          <span>{folderCount} {t("sendSummaryFolders")}</span>
        </div>
      </section>

      <section className="send-panel">
        <div
          className={`send-unified ${entries.length > 0 ? "has-items" : ""} ${
            isDropHovering ? "drag-hover" : ""
          }`}
          role="button"
          tabIndex={0}
          aria-label={t("sendScreenTitle")}
          onClick={onOpenPicker}
          onKeyDown={handleKeyDown}
          onDragOver={onDragOver}
          onDragEnter={onDragEnter}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
        >
          {entries.length > 0 ? (
            <>
              <p className="send-drop-hint">
                <i className="fa-solid fa-circle-plus"></i>
                <span>{t("sendDropMoreHint")}</span>
              </p>
              <ul className="send-list">
                {entries.map((entry, index) => (
                  <li className="send-item" key={`${entry.isDirectory}:${entry.path}`}>
                    <div className="send-item-main">
                      <i
                        className={`fa-solid ${entry.isDirectory ? "fa-folder" : "fa-file"}`}
                        aria-hidden="true"
                      ></i>
                      <div className="send-item-text">
                        <span className="send-item-path">{entry.path}</span>
                        <span className="send-item-size">
                          {entry.isDirectory
                            ? t("sendSizeUnknown")
                            : formatSize(entry.size ?? 0)}
                        </span>
                      </div>
                    </div>
                    <button
                      className="send-item-remove"
                      type="button"
                      aria-label="Remove item"
                      onClick={(event) => {
                        event.stopPropagation()
                        onRemove(index)
                      }}
                    >
                      <i className="fa-solid fa-trash"></i>
                    </button>
                  </li>
                ))}
              </ul>
            </>
          ) : (
            <div className="send-empty-hero">
              <i className="fa-solid fa-square-share-nodes" aria-hidden="true"></i>
              <h1>{t("sendScreenTitle")}</h1>
              <p>{t("sendScreenBody")}</p>
              <span>{t("sendScreenHint")}</span>
            </div>
          )}

          {isDropHovering ? (
            <div className="drop-zone-overlay">
              <i className="fa-solid fa-cloud-arrow-up"></i>
              <span>{t("sendDropOverlay")}</span>
            </div>
          ) : null}
        </div>
      </section>

      {entries.length > 0 ? (
        <button className="confirm-send-btn" type="button" onClick={onConfirm} disabled={isStarting}>
          <i className="fa-solid fa-paper-plane" aria-hidden="true"></i>
          <span>{t("sendNow")}</span>
        </button>
      ) : null}
    </main>
  )
}
