import { t } from "../strings"

interface HomeScreenProps {
  onGoSend: () => void
  onGoReceive: () => void
  onGoSettings: () => void
  onCopyPcCode: () => void
  onSharePcCode: () => void
  onRegeneratePcCode: () => void
  versionString: string
  pcJoinCode: string
  randomJoinCodeMode: boolean
}

export function HomeScreen({
  onGoSend,
  onGoReceive,
  onGoSettings,
  onCopyPcCode,
  onSharePcCode,
  onRegeneratePcCode,
  versionString,
  pcJoinCode,
  randomJoinCodeMode,
}: HomeScreenProps): JSX.Element {
  return (
    <main className="home-shell">
      <section className="choices">
        <button className="choice send" type="button" aria-label={t("sendTitle")} onClick={onGoSend}>
          <div className="choice-head">
            <h1 className="choice-title">{t("sendTitle")}</h1>
          </div>
          <div className="choice-mid">
            <span className="choice-icon" aria-hidden="true">
              <i className="fa-solid fa-square-share-nodes"></i>
            </span>
          </div>
          <p className="choice-body">{t("sendBody")}</p>
        </button>

        <button className="choice receive" type="button" aria-label={t("receiveTitle")} onClick={onGoReceive}>
          <div className="choice-head">
            <h1 className="choice-title">{t("receiveTitle")}</h1>
          </div>
          <div className="choice-mid">
            <span className="choice-icon" aria-hidden="true">
              <i className="fa-solid fa-envelope"></i>
            </span>
          </div>
          <p className="choice-body">{t("receiveBody")}</p>
        </button>
      </section>

      <section className="settings-panel">
        <div className="settings-panel-text">
          <h2>{t("thisPcCodeTitle")}</h2>
          <p>{randomJoinCodeMode ? t("thisPcCodeRandomBody") : t("thisPcCodeBody")}</p>
        </div>
        <div className="home-code-wrap">
          <div className={`home-code-box ${randomJoinCodeMode ? "muted" : ""}`}>
            {randomJoinCodeMode ? "RANDOM" : pcJoinCode}
          </div>
          <button
            className="home-code-btn"
            type="button"
            onClick={onCopyPcCode}
            disabled={randomJoinCodeMode}
            title={t("copyPcCode")}
          >
            <i className="fa-solid fa-copy"></i>
          </button>
          <button
            className="home-code-btn"
            type="button"
            onClick={onSharePcCode}
            disabled={randomJoinCodeMode}
            title={t("sharePcCode")}
          >
            <i className="fa-solid fa-share-nodes"></i>
          </button>
          <button
            className="home-code-btn"
            type="button"
            onClick={onRegeneratePcCode}
            disabled={randomJoinCodeMode}
            title={t("regeneratePcCode")}
          >
            <i className="fa-solid fa-rotate"></i>
          </button>
        </div>
        <button className="settings-open-btn" type="button" onClick={onGoSettings}>
          <i className="fa-solid fa-sliders"></i>
          <span>{t("openSettings")}</span>
        </button>
      </section>
      <a
        className="desktop-download-panel"
        href="https://thruflux.bytepipe.app/"
        target="_blank"
        rel="noreferrer"
      >
        <i className="fa-solid fa-desktop" aria-hidden="true"></i>
        <span className="desktop-download-copy">
          <strong>Move files between any devices. Get Thruflux for</strong>
          <small>Mac • Windows • Linux • Android</small>
        </span>
      </a>
      <p className="home-version-note">
        <span>{versionString}</span>
        <span>{t("appMadeBy")}</span>
      </p>
    </main>
  )
}
