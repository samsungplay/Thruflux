import {
  MAX_RECEIVERS_MAX,
  MAX_RECEIVERS_MIN,
  QCW_MAX,
  QCW_MIN,
  QSW_MAX,
  QSW_MIN,
  UDP_MAX,
  UDP_MIN,
} from "../constants"
import { t } from "../strings"
import { formatSize } from "../utils"
import type { SettingsErrors, SettingsState } from "../types"

interface SettingsScreenProps {
  state: SettingsState
  errors: SettingsErrors
  onBack: () => void
  onPatch: (patch: Partial<SettingsState>) => void
}

export function SettingsScreen({
  state,
  errors,
  onBack,
  onPatch,
}: SettingsScreenProps): JSX.Element {
  return (
    <main className="settings-shell">
      <button className="back-btn" type="button" onClick={onBack}>
        <i className="fa-solid fa-house"></i>
        <span>{t("backHome")}</span>
      </button>
      <section className="settings-card">
        <h1>{t("settingsTitle")}</h1>
        <p className="settings-warning">
          <i className="fa-solid fa-triangle-exclamation"></i>
          <span>{t("settingsWarning")}</span>
        </p>

        <label className="field-block">
          <span>{t("serverUrl")}</span>
          <input
            type="text"
            value={state.serverUrl}
            onChange={(e) => onPatch({ serverUrl: e.currentTarget.value })}
          />
          <small className="field-hint">{t("serverUrlHint")}</small>
          <small className="field-error">{errors.serverUrl ?? ""}</small>
        </label>

        <label className="field-block">
          <span>{t("stunServer")}</span>
          <input
            type="text"
            value={state.stunServer}
            onChange={(e) => onPatch({ stunServer: e.currentTarget.value })}
          />
          <small className="field-hint">{t("stunServerHint")}</small>
          <small className="field-error">{errors.stunServer ?? ""}</small>
        </label>

        <label className="field-block">
          <span>{t("turnServers")}</span>
          <textarea
            rows={4}
            value={state.turnServers}
            onChange={(e) => onPatch({ turnServers: e.currentTarget.value })}
          ></textarea>
          <small className="field-hint">{t("turnServersHint")}</small>
          <small className="field-error">{errors.turnServers ?? ""}</small>
        </label>

        <label className="toggle-row">
          <input
            type="checkbox"
            checked={state.forceTurn}
            onChange={(e) => onPatch({ forceTurn: e.currentTarget.checked })}
          />
          <span>{t("forceTurn")}</span>
        </label>
        <small className="field-hint">{t("forceTurnHint")}</small>

        <label className="field-block">
          <span>{t("quicConnWindowBytes")}</span>
          <input
            type="range"
            min={QCW_MIN}
            max={QCW_MAX}
            step={1024 * 1024}
            value={state.quicConnWindowBytes}
            onChange={(e) => {
              const next = Number.parseInt(e.currentTarget.value, 10)
              onPatch({
                quicConnWindowBytes: next,
                quicStreamWindowBytes:
                  state.quicStreamWindowBytes > next
                    ? next
                    : state.quicStreamWindowBytes,
              })
            }}
          />
          <small className="field-hint">{t("qcwHint")}</small>
          <small className="field-value">{formatSize(state.quicConnWindowBytes)}</small>
        </label>

        <label className="field-block">
          <span>{t("quicStreamWindowBytes")}</span>
          <input
            type="range"
            min={QSW_MIN}
            max={QSW_MAX}
            step={256 * 1024}
            value={state.quicStreamWindowBytes}
            onChange={(e) => {
              const next = Number.parseInt(e.currentTarget.value, 10)
              onPatch({
                quicStreamWindowBytes:
                  next > state.quicConnWindowBytes ? state.quicConnWindowBytes : next,
              })
            }}
          />
          <small className="field-hint">{t("qswHint")}</small>
          <small className="field-value">{formatSize(state.quicStreamWindowBytes)}</small>
          <small className="field-error">{errors.quicRelation ?? ""}</small>
        </label>

        <label className="field-block">
          <span>{t("udpBufferBytes")}</span>
          <input
            type="range"
            min={UDP_MIN}
            max={UDP_MAX}
            step={1024 * 1024}
            value={state.udpBufferBytes}
            onChange={(e) =>
              onPatch({ udpBufferBytes: Number.parseInt(e.currentTarget.value, 10) })
            }
          />
          <small className="field-hint">{t("udpHint")}</small>
          <small className="field-value">{formatSize(state.udpBufferBytes)}</small>
        </label>

        <label className="field-block">
          <span>{t("maxReceivers")}</span>
          <input
            type="range"
            min={MAX_RECEIVERS_MIN}
            max={MAX_RECEIVERS_MAX}
            step={1}
            value={state.maxReceivers}
            onChange={(e) =>
              onPatch({ maxReceivers: Number.parseInt(e.currentTarget.value, 10) })
            }
          />
          <small className="field-hint">{t("maxReceiversHint")}</small>
          <small className="field-value">{state.maxReceivers}</small>
        </label>

        <label className="toggle-row">
          <input
            type="checkbox"
            checked={state.overwrite}
            onChange={(e) => onPatch({ overwrite: e.currentTarget.checked })}
          />
          <span>{t("overwrite")}</span>
        </label>
        <small className="field-hint">{t("overwriteHint")}</small>
        <small className="field-hint">{t("autoResumeHint")}</small>

        <h2 className="settings-subtitle">{t("notificationSettingsTitle")}</h2>

        <label className="toggle-row">
          <input
            type="checkbox"
            checked={state.notifyReceiverSessionComplete}
            onChange={(e) =>
              onPatch({ notifyReceiverSessionComplete: e.currentTarget.checked })
            }
          />
          <span>{t("notificationReceiverFinishedLabel")}</span>
        </label>
        <small className="field-hint">{t("notificationReceiverFinishedHint")}</small>

        <label className="toggle-row">
          <input
            type="checkbox"
            checked={state.notifySenderReceiverJoined}
            onChange={(e) =>
              onPatch({ notifySenderReceiverJoined: e.currentTarget.checked })
            }
          />
          <span>{t("notificationSenderJoinedLabel")}</span>
        </label>
        <small className="field-hint">{t("notificationSenderJoinedHint")}</small>

        <label className="toggle-row">
          <input
            type="checkbox"
            checked={state.notifySenderReceiverComplete}
            onChange={(e) =>
              onPatch({ notifySenderReceiverComplete: e.currentTarget.checked })
            }
          />
          <span>{t("notificationSenderCompletedLabel")}</span>
        </label>
        <small className="field-hint">{t("notificationSenderCompletedHint")}</small>

        <label className="toggle-row">
          <input
            type="checkbox"
            checked={state.notifyTransferFailure}
            onChange={(e) =>
              onPatch({ notifyTransferFailure: e.currentTarget.checked })
            }
          />
          <span>{t("notificationTransferFailedLabel")}</span>
        </label>
        <small className="field-hint">{t("notificationTransferFailedHint")}</small>
      </section>
    </main>
  )
}
