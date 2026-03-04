import { t } from "../strings"
import type { HealthState, Theme, ThemePreference } from "../types"

interface TopBarProps {
  healthState: HealthState
  theme: Theme
  onSetThemePreference: (value: ThemePreference) => void
}

export function TopBar({
  healthState,
  theme,
  onSetThemePreference,
}: TopBarProps): JSX.Element {
  return (
    <header className="topbar">
      <section className="status" aria-live="polite">
        <span className={`status-dot ${healthState}`} id="status-dot"></span>
        <div className="status-text">
          <span className="status-label">{t("statusLabel")}</span>
          <span id="status-value">
            {healthState === "success"
              ? t("statusReady")
              : healthState === "failed"
                ? t("statusNotReady")
                : t("statusChecking")}
          </span>
        </div>
      </section>

      <div className="brand" aria-label={t("appName")}>
        <div className="brand-main">
          <i className="fa-solid fa-bolt" aria-hidden="true"></i>
          <span>{t("appName")}</span>
        </div>
        <span className="brand-slogan">{t("appSlogan")}</span>
      </div>

      <div className="theme-toggle" role="group" aria-label="Theme">
        <button
          className={`theme-option ${theme === "light" ? "active" : ""}`}
          onClick={() => onSetThemePreference("light")}
        >
          {t("themeLight")}
        </button>
        <button
          className={`theme-option ${theme === "dark" ? "active" : ""}`}
          onClick={() => onSetThemePreference("dark")}
        >
          {t("themeDark")}
        </button>
      </div>
    </header>
  )
}
