import { t } from "../strings"
import type {
  ReceiveFlowStage,
  ReceiveTransferProgressState,
  SavedPc,
} from "../types"
import { formatEta, formatSize, formatThroughput } from "../utils"

interface ReceiveScreenProps {
  joinCode: string
  saveDirectory: string
  isDirectoryValid: boolean
  overwrite: boolean
  flowStage: ReceiveFlowStage
  manifestTotalSize: number
  manifestSummaryFilesCount: number
  manifestSummaryTotalSize: number
  transferProgress: ReceiveTransferProgressState
  savedPcs: SavedPc[]
  onBack: () => void
  onJoinCodeChange: (value: string) => void
  onSelectSavedPc: (savedPc: SavedPc) => void
  onSaveCurrentPc: () => void
  onRemoveSavedPc: (id: string) => void
  onSelectDirectory: () => void
  onOverwriteChange: (value: boolean) => void
  onReceive: () => void
  onAbort: () => void
  onOpenSaveFolder: () => void
  onRetry: () => void
}

export function ReceiveScreen({
  joinCode,
  saveDirectory,
  isDirectoryValid,
  overwrite,
  flowStage,
  manifestTotalSize,
  manifestSummaryFilesCount,
  manifestSummaryTotalSize,
  transferProgress,
  savedPcs,
  onBack,
  onJoinCodeChange,
  onSelectSavedPc,
  onSaveCurrentPc,
  onRemoveSavedPc,
  onSelectDirectory,
  onOverwriteChange,
  onReceive,
  onAbort,
  onOpenSaveFolder,
  onRetry,
}: ReceiveScreenProps): JSX.Element {
  if (flowStage !== "idle") {
    return (
      <main className="receive-shell state-mode">
        <button className="back-btn" type="button" onClick={onBack}>
          <i className="fa-solid fa-house"></i>
          <span>{t("backHome")}</span>
        </button>

        <section
          className={`send-state-card ${
            flowStage === "transfer" || flowStage === "complete" || flowStage === "failed"
              ? "receive-transfer-mode"
              : ""
          }`}
        >
          {flowStage === "starting" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-gear"></i>
              </span>
              <h1>{t("receiveStartingTitle")}</h1>
              <p>{t("receiveStartingBody")}</p>
            </>
          ) : null}

          {flowStage === "connecting" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-cloud"></i>
              </span>
              <h1>{t("receiveConnectingTitle")}</h1>
              <p>{t("receiveConnectingBody")}</p>
            </>
          ) : null}

          {flowStage === "connected" ? (
            <>
              <span className="state-icon success">
                <i className="fa-solid fa-circle-check"></i>
              </span>
              <h1>{t("receiveConnectedTitle")}</h1>
              <p>{t("receiveConnectedBody")}</p>
            </>
          ) : null}

          {flowStage === "joining_session" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-right-to-bracket"></i>
              </span>
              <h1>{t("receiveJoiningTitle")}</h1>
              <p>{t("receiveJoiningBody")}</p>
            </>
          ) : null}

          {flowStage === "p2p_start" ? (
            <>
              <span className="state-icon spin">
                <i className="fa-solid fa-link"></i>
              </span>
              <h1>{t("receiveP2PStartTitle")}</h1>
              <p>{t("receiveP2PStartBody")}</p>
            </>
          ) : null}

          {flowStage === "p2p_success" ? (
            <>
              <span className="state-icon success">
                <i className="fa-solid fa-link"></i>
              </span>
              <h1>{t("receiveP2PSuccessTitle")}</h1>
              <p>{t("receiveP2PSuccessBody")}</p>
            </>
          ) : null}

          {flowStage === "manifest_receiving" ? (
            <div className="manifest-shell">
              <span className="state-icon spin">
                <i className="fa-solid fa-file-lines"></i>
              </span>
              <h1>{t("receiveManifestTitle")}</h1>
              <p>{t("receiveManifestBody")}</p>
              <div className="manifest-progress-track">
                <div className="manifest-progress-fill indeterminate"></div>
              </div>
              <div className="manifest-metrics">
                <span>
                  {t("receiveManifestSizeLabel")}: {formatSize(manifestTotalSize)}
                </span>
              </div>
            </div>
          ) : null}

          {flowStage === "manifest_parsing" ? (
            <div className="manifest-shell">
              <span className="state-icon spin">
                <i className="fa-solid fa-list-check"></i>
              </span>
              <h1>{t("receiveManifestParsingTitle")}</h1>
              <p>{t("receiveManifestParsingBody")}</p>
              <div className="manifest-progress-track">
                <div className="manifest-progress-fill indeterminate"></div>
              </div>
            </div>
          ) : null}

          {flowStage === "quic_ready" ? (
            <>
              <span className="state-icon success">
                <i className="fa-solid fa-shield-halved"></i>
              </span>
              <h1>{t("receiveSecureLinkTitle")}</h1>
              <p>{t("receiveSecureLinkBody")}</p>
            </>
          ) : null}

          {flowStage === "transfer" || flowStage === "complete" || flowStage === "failed" ? (
            <div className="receive-transfer-shell">
              <h1>
                {flowStage === "complete"
                  ? t("receiveCompleteTitle")
                  : flowStage === "failed"
                    ? t("receiveTransferFailedTitle")
                  : t("receiveTransferTitle")}
              </h1>
              <p>
                {flowStage === "complete"
                  ? t("receiveCompleteBody")
                  : flowStage === "failed"
                    ? t("receiveTransferFailedBody")
                  : t("receiveTransferBody")}
              </p>
              {flowStage === "complete" ? (
                <div className="receive-complete-badge">
                  <i className="fa-solid fa-circle-check"></i>
                  <span>{t("receiveCompleteTitle")}</span>
                </div>
              ) : null}
              {flowStage === "failed" ? (
                <div className="receive-failed-badge">
                  <i className="fa-solid fa-triangle-exclamation"></i>
                  <span>{t("receiveFailedBadge")}</span>
                </div>
              ) : null}
              <div className="manifest-final-summary receive-final-summary">
                <div className="receive-final-summary-row">
                  <span>{t("receiveExpectedFilesLabel")}</span>
                  <strong>{manifestSummaryFilesCount}</strong>
                </div>
                <div className="receive-final-summary-row">
                  <span>{t("receiveExpectedSizeLabel")}</span>
                  <strong>{formatSize(manifestSummaryTotalSize)}</strong>
                </div>
                <div className="receive-final-summary-row">
                  <span>{t("receiveSaveDirectoryLabel")}</span>
                  <strong className="receive-final-summary-path">{saveDirectory}</strong>
                </div>
              </div>
              <div className="transfer-progress-head">
                <span>{t("receivePercentLabel")}</span>
                <strong>{Math.round(transferProgress.percent)}%</strong>
              </div>
              <div className="manifest-progress-track">
                <div
                  className="manifest-progress-fill"
                  style={{ width: `${Math.max(0, Math.min(100, transferProgress.percent))}%` }}
                ></div>
              </div>
              <div className="receive-transfer-metrics">
                <div>
                  <span>{t("receiveThroughputLabel")}</span>
                  <strong>{formatThroughput(transferProgress.ewmaThroughput)}</strong>
                </div>
                <div>
                  <span>{t("receiveMovedLabel")}</span>
                  <strong>{formatSize(transferProgress.bytesMoved)}</strong>
                </div>
                <div>
                  <span>{t("receiveSkippedLabel")}</span>
                  <strong>{formatSize(transferProgress.skippedBytes)}</strong>
                </div>
                <div>
                  <span>{t("receiveFilesDoneLabel")}</span>
                  <strong>
                    {transferProgress.filesMoved}/{transferProgress.totalExpectedFilesCount}
                  </strong>
                </div>
                <div>
                  <span>{t("receiveRouteLabel")}</span>
                  <strong>
                    {transferProgress.isRelayed
                      ? t("receiveRouteRelayed")
                      : t("receiveRouteDirect")}
                  </strong>
                </div>
                <div>
                  <span>{t("receiveEtaLabel")}</span>
                  <strong>
                    {formatEta(
                      manifestSummaryTotalSize,
                      transferProgress.bytesMoved,
                      transferProgress.skippedBytes,
                      transferProgress.ewmaThroughput,
                    )}
                  </strong>
                </div>
              </div>
            </div>
          ) : null}

          {flowStage === "transfer" ? (
            <button className="receive-abort-btn" type="button" onClick={onAbort}>
              <i className="fa-solid fa-stop"></i>
              <span>{t("receiveAbortButton")}</span>
            </button>
          ) : null}

          {flowStage === "complete" ? (
            <button className="receive-open-folder-btn" type="button" onClick={onOpenSaveFolder}>
              <i className="fa-solid fa-folder-open"></i>
              <span>{t("receiveOpenFolderButton")}</span>
            </button>
          ) : null}

          {flowStage === "failed" ? (
            <button className="receive-open-folder-btn" type="button" onClick={onRetry}>
              <i className="fa-solid fa-rotate-right"></i>
              <span>{t("receiveRetryButton")}</span>
            </button>
          ) : null}
        </section>
      </main>
    )
  }

  return (
    <main className="receive-shell">
      <button className="back-btn" type="button" onClick={onBack}>
        <i className="fa-solid fa-house"></i>
        <span>{t("backHome")}</span>
      </button>

      <section className="receive-card">
        <span className="receive-icon" aria-hidden="true">
          <i className="fa-solid fa-envelope-open-text"></i>
        </span>
        <h1>{t("receiveScreenTitle")}</h1>
        <p>{t("receiveScreenBody")}</p>

        <label className="receive-field" htmlFor="receive-join-code">
          <span>{t("receiveJoinCodeLabel")}</span>
          <div className="receive-code-row">
            <input
              id="receive-join-code"
              type="text"
              value={joinCode}
              onChange={(event) => onJoinCodeChange(event.currentTarget.value)}
              placeholder={t("receiveJoinCodePlaceholder")}
              autoComplete="off"
              spellCheck={false}
            />
            <button className="receive-save-pc-btn" type="button" onClick={onSaveCurrentPc}>
              <i className="fa-solid fa-bookmark"></i>
              <span>{t("savePc")}</span>
            </button>
          </div>
          <small>{t("receiveJoinCodeHint")}</small>
        </label>

        <section className="saved-pcs-panel">
          <div className="saved-pcs-head">
            <h2>{t("savedPcsTitle")}</h2>
          </div>
          {savedPcs.length > 0 ? (
            <div className="saved-pcs-list">
              {savedPcs.map((savedPc) => (
                <div className="saved-pc-item" key={savedPc.id}>
                  <button
                    className="saved-pc-main"
                    type="button"
                    onClick={() => onSelectSavedPc(savedPc)}
                  >
                    <span>{savedPc.name}</span>
                    <strong>{savedPc.joinCode}</strong>
                  </button>
                  <button
                    className="saved-pc-remove"
                    type="button"
                    onClick={() => onRemoveSavedPc(savedPc.id)}
                    title={t("removeSavedPc")}
                    aria-label={t("removeSavedPc")}
                  >
                    <i className="fa-solid fa-trash"></i>
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <p className="saved-pcs-empty">{t("savedPcsEmpty")}</p>
          )}
        </section>

        <label className="receive-field" htmlFor="receive-save-directory">
          <span>{t("receiveSaveDirectoryLabel")}</span>
          <div className="receive-dir-row">
            <input
              id="receive-save-directory"
              type="text"
              value={saveDirectory}
              readOnly
              placeholder="./downloads"
              autoComplete="off"
              spellCheck={false}
            />
            <button
              className="receive-dir-btn"
              type="button"
              onClick={onSelectDirectory}
            >
              {t("receiveSaveDirectoryButton")}
            </button>
          </div>
          <small>{t("receiveSaveDirectoryHint")}</small>
        </label>

        <section className="receive-toggle-card">
          <div>
            <h2>{t("overwrite")}</h2>
            <p>{t("receiveOverwriteBody")}</p>
          </div>
          <label className="toggle-row">
            <input
              type="checkbox"
              checked={overwrite}
              onChange={(event) => onOverwriteChange(event.currentTarget.checked)}
            />
          </label>
        </section>

        <button
          className="receive-action-btn"
          type="button"
          disabled={!isDirectoryValid}
          onClick={onReceive}
        >
          <i className="fa-solid fa-inbox"></i>
          <span>{t("receiveActionButton")}</span>
        </button>
      </section>
    </main>
  )
}
