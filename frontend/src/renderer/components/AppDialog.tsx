import { t } from "../strings"
import type { AppDialogState } from "../types"

interface AppDialogProps {
  dialog: AppDialogState | null
  onClose: () => void
}

export function AppDialog({ dialog, onClose }: AppDialogProps): JSX.Element | null {
  if (!dialog) {
    return null
  }

  return (
    <div className="dialog-backdrop" role="presentation" onClick={onClose}>
      <section
        className={`dialog-card tone-${dialog.tone}`}
        role="dialog"
        aria-modal="true"
        aria-label={dialog.title}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="dialog-accent" aria-hidden="true"></div>
        <h2>{dialog.title}</h2>
        <p>{dialog.message}</p>
        <div className="dialog-actions">
          {dialog.actionLabel ? (
            <button className="dialog-btn" type="button" onClick={dialog.onAction}>
              {dialog.actionLabel}
            </button>
          ) : null}
          <button className="dialog-btn" type="button" onClick={onClose}>
            {t("dismiss")}
          </button>
        </div>
      </section>
    </div>
  )
}
