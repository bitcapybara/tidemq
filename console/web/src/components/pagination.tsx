import { useState } from "react";
import { Button } from "./ui/button";
import { useLocale } from "@/locales/locale";

export function usePageCursor() {
  const [history, setHistory] = useState<(string | undefined)[]>([undefined]);
  return {
    cursor: history.at(-1),
    reset: () => setHistory([undefined]),
    next: (cursor: string) => setHistory((previous) => [...previous, cursor]),
    previous: () =>
      setHistory((previous) =>
        previous.length > 1 ? previous.slice(0, -1) : previous,
      ),
    hasPrevious: history.length > 1,
  };
}

export function Pagination({
  cursor,
  nextCursor,
  busy,
}: {
  cursor: ReturnType<typeof usePageCursor>;
  nextCursor?: string | null;
  busy?: boolean;
}) {
  const { t } = useLocale();
  return (
    <div className="inline-actions">
      <Button
        type="button"
        size="sm"
        variant="outline"
        disabled={!cursor.hasPrevious || busy}
        onClick={cursor.previous}
      >
        {t("Previous", "上一页")}
      </Button>
      <Button
        type="button"
        size="sm"
        variant="outline"
        disabled={!nextCursor || busy}
        onClick={() => nextCursor && cursor.next(nextCursor)}
      >
        {t("Next page", "下一页")}
      </Button>
    </div>
  );
}
