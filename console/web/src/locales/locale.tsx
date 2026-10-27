import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

type Language = "en" | "zh-CN";
type Translator = (english: string, chinese: string) => string;
const LocaleContext = createContext<{
  language: Language;
  setLanguage: (language: Language) => void;
  t: Translator;
}>({
  language: "en",
  setLanguage: () => undefined,
  t: (english) => english,
});

function initialLanguage(): Language {
  try {
    const stored = localStorage.getItem("tidemq.language");
    if (stored === "en" || stored === "zh-CN") return stored;
  } catch {
    /* Language preference is optional when storage is unavailable. */
  }
  return navigator.language.startsWith("zh") ? "zh-CN" : "en";
}

export function LocaleProvider({ children }: { children: ReactNode }) {
  const [language, setLanguage] = useState<Language>(initialLanguage);
  useEffect(() => {
    document.documentElement.lang = language;
    try {
      localStorage.setItem("tidemq.language", language);
    } catch {
      /* Storage is optional. */
    }
  }, [language]);
  return (
    <LocaleContext
      value={{
        language,
        setLanguage,
        t: (english, chinese) => (language === "zh-CN" ? chinese : english),
      }}
    >
      {children}
    </LocaleContext>
  );
}

export const useLocale = () => useContext(LocaleContext);
