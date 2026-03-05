import { t } from "../strings"

interface HomeScreenProps {
  onGoSend: () => void
  onGoReceive: () => void
  onGoSettings: () => void
  versionString: string
}

export function HomeScreen({
  onGoSend,
  onGoReceive,
  onGoSettings,
  versionString,
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
          <h2>{t("settingsPanelTitle")}</h2>
          <p>{t("settingsPanelBody")}</p>
        </div>
        <button className="settings-open-btn" type="button" onClick={onGoSettings}>
          <i className="fa-solid fa-sliders"></i>
          <span>{t("openSettings")}</span>
        </button>
      </section>
      <p className="home-version-note">
        <span>{versionString}</span>
        <span>{t("appMadeBy")}</span>
      </p>
    </main>
  )
}
