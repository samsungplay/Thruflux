export type Locale = "en";

export type TranslationKey =
  | "appName"
  | "tagline"
  | "startLabel"
  | "startHint"
  | "statusReady"
  | "statusStarting"
  | "statusNotReady"
  | "sendTitle"
  | "sendBody"
  | "receiveTitle"
  | "receiveBody"
  | "openSettings"
  | "lastSession"
  | "themeLight"
  | "themeDark";

type TranslationTable = Record<TranslationKey, string>;

const translations: Record<Locale, TranslationTable> = {
  en: {
    appName: "Thruflux",
    tagline: "Move big files with ease",
    startLabel: "Status",
    startHint: "Getting things ready",
    statusReady: "Ready",
    statusStarting: "Starting",
    statusNotReady: "Not ready",
    sendTitle: "Send",
    sendBody: "Share files with others in one go",
    receiveTitle: "Receive",
    receiveBody: "Get files quickly with one code",
    openSettings: "Settings",
    lastSession: "Last transfer",
    themeLight: "Light",
    themeDark: "Dark",
  },
};

export function resolveLocale(): Locale {
  return "en";
}

export function t(locale: Locale, key: TranslationKey): string {
  return translations[locale][key];
}
