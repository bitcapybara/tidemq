import { useId, useState } from "react";
import { useLocale } from "@/locales/locale";

type Sample = { at: string; exact: string | null; value: number | null };

export function MetricTrend({
  name,
  value,
  observedAt,
  unit,
}: {
  name: string;
  value: string | null;
  observedAt: string;
  unit?: string | null;
}) {
  const { t, language } = useLocale();
  const titleId = useId();
  const [samples, setSamples] = useState<Sample[]>([]);
  const numeric = value === null ? NaN : Number(value);
  const plotValue =
    Number.isFinite(numeric) && Math.abs(numeric) <= Number.MAX_SAFE_INTEGER
      ? numeric
      : null;
  if (samples.at(-1)?.at !== observedAt)
    setSamples([
      ...samples.slice(-59),
      { at: observedAt, exact: value, value: plotValue },
    ]);
  const values = samples.flatMap((sample) =>
    sample.value === null ? [] : [sample.value],
  );
  const lower = values.length ? Math.min(...values) : null;
  const upper = values.length ? Math.max(...values) : null;
  const firstTime = samples.at(0)?.at;
  const lastTime = samples.at(-1)?.at;
  const duration =
    firstTime && lastTime
      ? Math.max(1, Date.parse(lastTime) - Date.parse(firstTime))
      : 1;
  const points: { x: number; y: number }[][] = [];
  let segment: { x: number; y: number }[] = [];
  for (const sample of samples) {
    if (sample.value === null || lower === null || upper === null) {
      if (segment.length) points.push(segment);
      segment = [];
      continue;
    }
    const x =
      12 +
      (276 * (Date.parse(sample.at) - Date.parse(firstTime ?? sample.at))) /
        duration;
    const y =
      upper === lower
        ? 56
        : 92 - (72 * (sample.value - lower)) / (upper - lower);
    segment.push({ x, y });
  }
  if (segment.length) points.push(segment);
  return (
    <figure className="metric-trend">
      <figcaption>
        <strong>{name}</strong>
        <span>
          {value ?? t("Unknown", "未知")} {unit}
        </span>
      </figcaption>
      <svg viewBox="0 0 300 112" role="img" aria-labelledby={titleId}>
        <title id={titleId}>
          {name} · {t("observed on this page", "本页观测")} · {samples.length}{" "}
          {t("samples", "次采样")}
        </title>
        <line x1="12" x2="288" y1="92" y2="92" stroke="#d2dfe3" />
        {points.map((line, index) => (
          <g key={index}>
            <polyline
              points={line.map((point) => `${point.x},${point.y}`).join(" ")}
              fill="none"
              stroke="#267e73"
              strokeWidth="2"
            />
            {line.map((point, pointIndex) => (
              <circle
                key={pointIndex}
                cx={point.x}
                cy={point.y}
                r="2.5"
                fill="#267e73"
              />
            ))}
          </g>
        ))}
        {lower !== null && (
          <text x="12" y="108" fontSize="9" fill="#526972">
            {lower.toLocaleString(language)} – {upper?.toLocaleString(language)}
          </text>
        )}
      </svg>
      <p>
        {t("This page only · no historical data", "仅本页观测 · 无历史数据")} ·{" "}
        {samples.length < 2
          ? t("waiting for another real sample", "等待下一次真实采样")
          : `${samples.length} ${t("samples", "次采样")}`}
      </p>
      <small>
        {firstTime && new Date(firstTime).toLocaleTimeString(language)} —{" "}
        {lastTime && new Date(lastTime).toLocaleTimeString(language)} ·{" "}
        {t(
          "Missing or oversized values are gaps, never zero.",
          "缺失或超出图表精度的值留空，不补零。",
        )}
      </small>
    </figure>
  );
}
