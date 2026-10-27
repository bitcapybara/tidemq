import { useRef, useState } from "react";
import { useSearchParams } from "react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "@/api/client";
import {
  messagePageSchema,
  resourceIdSchema,
  type Message,
  type MessageQuery,
  type SendMessageInput,
} from "@/api/types";
import { useSession } from "@/api/session";
import {
  DataTable,
  FailureNotice,
  ObservationLabel,
  Panel,
  ReadState,
} from "@/components/console";
import { Field, WriteForm, field } from "@/components/forms";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useLocale } from "@/locales/locale";
import {
  displayBytes,
  formatDecimal,
  isOffset,
  parseProperties,
  toBase64,
  validateMessageBudget,
} from "./logic";

type Shard = {
  id: string;
  sealed: boolean | null;
  retainedStart: string | null;
  committedEnd: string | null;
};

export function MessageBrowser({
  topicId,
  shards,
  allowed,
}: {
  topicId: string;
  shards: Shard[];
  allowed: boolean;
}) {
  const { t } = useLocale();
  const session = useSession();
  const [search, setSearch] = useSearchParams();
  const selection = search.get("shard") ?? "";
  const invalidSelection = Boolean(
    selection &&
    selection !== "all" &&
    !resourceIdSchema.safeParse(selection).success,
  );
  const [expanded, setExpanded] = useState<string | null>(null);
  const available: Shard[] =
    selection &&
    selection !== "all" &&
    resourceIdSchema.safeParse(selection).success &&
    !shards.some((item) => item.id === selection)
      ? [
          ...shards,
          {
            id: selection,
            sealed: null,
            retainedStart: null,
            committedEnd: null,
          },
        ]
      : shards;
  const shard =
    selection && selection !== "all"
      ? available.find((item) => item.id === selection)
      : available.at(0);
  if (session.session && !allowed)
    return (
      <FailureNotice
        error={new ApiError("forbidden", "content_inspection_required")}
      />
    );
  return (
    <>
      <Panel
        title={t("Bounded message inspection", "有界消息排查")}
        description={t(
          "Hot and archived messages. Offsets are ordered only within one shard. Exact filters use AND. No timestamp ordering is implied.",
          "覆盖热数据与归档数据，仅在单个 Shard 内按 offset 排序。精确筛选条件之间使用 AND，不暗示时间顺序。",
        )}
      >
        <div className="filter-bar">
          <label>
            <span>{t("Shard view", "Shard 视图")}</span>
            <select
              value={
                selection === "all" || invalidSelection
                  ? selection
                  : (shard?.id ?? "")
              }
              onChange={(event) =>
                setSearch((previous) => {
                  previous.set("shard", event.target.value);
                  previous.delete("offset");
                  return previous;
                })
              }
            >
              <option value="all">
                {t("Group loaded shards", "已加载 Shard 分组")}
              </option>
              {invalidSelection && (
                <option value={selection}>
                  {t("Invalid shard identifier", "无效的 Shard 标识")}
                </option>
              )}
              {available.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.id} ·{" "}
                  {item.sealed === null
                    ? t("not in loaded topology", "不在已加载拓扑中")
                    : item.sealed
                      ? t("sealed", "封存")
                      : t("current", "当前")}
                </option>
              ))}
            </select>
          </label>
        </div>
        {invalidSelection ? (
          <FailureNotice
            error={new ApiError("invalid", "invalid_shard_identifier")}
            compact
          />
        ) : selection === "all" ? (
          <div className="shard-groups">
            <p className="footnote">
              {t(
                "Open one group to inspect it. No global offset or ordering exists across shards; unopened groups perform no scan.",
                "展开单个分组后进行排查。跨 Shard 没有统一 offset 或顺序，未展开分组不会发起扫描。",
              )}
            </p>
            {shards.map((item) => (
              <section key={item.id} className="shard-group">
                <button
                  type="button"
                  className="shard-toggle"
                  aria-expanded={expanded === item.id}
                  onClick={() =>
                    setExpanded(expanded === item.id ? null : item.id)
                  }
                >
                  {item.id} ·{" "}
                  {item.sealed ? t("sealed", "封存") : t("current", "当前")}
                </button>
                {expanded === item.id && (
                  <ShardMessages
                    key={item.id}
                    topicId={topicId}
                    shard={item}
                    allowed={allowed}
                  />
                )}
              </section>
            ))}
          </div>
        ) : shard ? (
          <ShardMessages
            key={`${shard.id}:${search.get("offset") ?? ""}`}
            topicId={topicId}
            shard={shard}
            allowed={allowed}
            initialOffset={
              search.get("shard") === shard.id ? search.get("offset") : null
            }
          />
        ) : (
          <ReadState empty={Boolean(session.session)} />
        )}
      </Panel>
    </>
  );
}

function ShardMessages({
  topicId,
  shard,
  allowed,
  initialOffset,
}: {
  topicId: string;
  shard: Shard;
  allowed: boolean;
  initialOffset?: string | null;
}) {
  const { t, language } = useLocale();
  const session = useSession();
  const client = useQueryClient();
  const [scan, setScan] = useState<{
    request: MessageQuery;
    serial: number;
  } | null>(null);
  const [selected, setSelected] = useState<Message | null>(null);
  const [history, setHistory] = useState<MessageQuery[]>([]);
  const [encoding, setEncoding] = useState("utf8");
  const [key, setKey] = useState("");
  const [keyEnabled, setKeyEnabled] = useState(false);
  const [properties, setProperties] = useState("{}");
  const [manualError, setManualError] = useState<unknown>(null);
  const [cancelled, setCancelled] = useState(false);
  const form = useRef<HTMLFormElement>(null);
  const request: MessageQuery = scan?.request ?? {
    shard: shard.id,
    limit: 32,
    startOffset:
      initialOffset && isOffset(initialOffset) ? initialOffset : undefined,
  };
  const invalidInitialOffset =
    initialOffset !== null &&
    initialOffset !== undefined &&
    !isOffset(initialOffset);
  const scope = `topic:${topicId}`;
  // Filter bytes remain in the request closure, never in query identifiers or URLs.
  const queryKey = [
    "protected",
    session.epoch,
    scope,
    "messages",
    shard.id,
    scan?.serial ?? 0,
  ];
  const query = useQuery({
    queryKey,
    queryFn: ({ signal }) =>
      api.request(`/api/v1/topics/${topicId}/messages`, messagePageSchema, {
        scope,
        method: "POST",
        body: request,
        signal,
      }),
    enabled:
      Boolean(session.session) &&
      allowed &&
      !session.denied.has(scope) &&
      (Boolean(scan) || !invalidInitialOffset),
    retry: false,
    refetchOnWindowFocus: false,
    gcTime: 0,
    staleTime: Infinity,
  });
  const data = query.data;
  const busy = query.isFetching;
  const error =
    manualError ??
    (!scan && invalidInitialOffset
      ? new ApiError("invalid", "invalid_offset_range")
      : query.error);
  function inspect(next: MessageQuery, previous = false) {
    if (!allowed || !session.session || busy) return;
    setManualError(null);
    setSelected(null);
    setCancelled(false);
    if (!previous) setHistory((values) => [...values.slice(-19), request]);
    setScan((value) => ({ request: next, serial: (value?.serial ?? 0) + 1 }));
  }
  function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const values = new FormData(event.currentTarget);
    const start = field(values, "start"),
      end = field(values, "end"),
      messageId = field(values, "messageId");
    if (
      (start && !isOffset(start)) ||
      (end && !isOffset(end)) ||
      (start && end && BigInt(start) >= BigInt(end))
    ) {
      setManualError(new ApiError("invalid", "invalid_offset_range"));
      return;
    }
    try {
      const next = {
        shard: shard.id,
        limit: 32,
        startOffset: start || undefined,
        endOffset: end || undefined,
        messageId: messageId || undefined,
        keyBase64: keyEnabled ? toBase64(key, encoding) : undefined,
        propertiesBase64: parseProperties(properties, encoding),
      };
      setHistory([]);
      inspect(next, true);
    } catch {
      setManualError(new ApiError("invalid", "invalid_binary_filter"));
    }
  }
  function context(offset: string) {
    if (!data) return;
    const center = BigInt(offset),
      floor = BigInt(data.retainedStart),
      end = BigInt(data.observedEnd);
    setKey("");
    setKeyEnabled(false);
    setProperties("{}");
    inspect({
      shard: shard.id,
      startOffset: (center - 16n < floor ? floor : center - 16n).toString(),
      endOffset: (center + 16n > end ? end : center + 16n).toString(),
      observedEnd: data.observedEnd,
      limit: 32,
    });
  }
  return (
    <>
      <div className="info-ribbon">
        <span>
          Shard <code>{shard.id}</code>
        </span>
        <span>
          {t("Retained start", "保留起点")}{" "}
          {data?.retainedStart ?? shard.retainedStart ?? "—"}
        </span>
        <span>
          {t("Observed end (exclusive)", "观察末端（排他）")}{" "}
          {data?.observedEnd ?? "—"}
        </span>
      </div>
      <form ref={form} className="message-form" onSubmit={submit}>
        <div className="form-grid">
          <Field
            name="start"
            label={t("Start offset (inclusive)", "起始 offset（包含）")}
            value={initialOffset ?? undefined}
          />
          <Field
            name="end"
            label={t("End offset (exclusive)", "结束 offset（排他）")}
          />
          <Field
            name="messageId"
            label={t(
              "MessageId (locate with context)",
              "MessageId（定位及上下文）",
            )}
          />
        </div>
        <details className="advanced-filter">
          <summary>
            {t("Exact key / property filters", "精确 key / 属性筛选")}{" "}
            <Badge variant="outline">AND</Badge>
          </summary>
          <div className="form-grid">
            <label>
              <span>{t("Value encoding", "值编码")}</span>
              <select
                value={encoding}
                onChange={(event) => setEncoding(event.target.value)}
              >
                <option value="utf8">UTF-8</option>
                <option value="base64">Base64</option>
              </select>
            </label>
            <label>
              <span className="check-label">
                <input
                  type="checkbox"
                  checked={keyEnabled}
                  onChange={(event) => setKeyEnabled(event.target.checked)}
                />
                {t(
                  "Match exact key (including empty)",
                  "精确匹配 key（可为空值）",
                )}
              </span>
              <Input
                aria-label={t("Exact key value", "精确 key 值")}
                value={key}
                onChange={(event) => setKey(event.target.value)}
                disabled={!keyEnabled}
                autoComplete="off"
              />
            </label>
            <label>
              <span>
                {t("Property object (string values)", "属性对象（值为字符串）")}
              </span>
              <Input
                value={properties}
                onChange={(event) => setProperties(event.target.value)}
                autoComplete="off"
              />
            </label>
          </div>
          <p>
            {t(
              "Missing properties are different from empty values. Filter contents stay in memory and never enter links. Matching always uses the original full bytes.",
              "缺失属性与空值不同。筛选内容仅保存在内存中，不进入链接；匹配始终使用原始完整字节。",
            )}
          </p>
        </details>
        <div className="inline-actions">
          <Button type="submit" disabled={!allowed || !session.session || busy}>
            {t("Inspect range", "开始排查")}
          </Button>
          <Button
            type="button"
            variant="outline"
            disabled={!allowed || !session.session || busy}
            onClick={() => {
              form.current?.reset();
              setHistory([]);
              setKey("");
              setKeyEnabled(false);
              setProperties("{}");
              inspect({ shard: shard.id, limit: 32 }, true);
            }}
          >
            {t("Latest window", "最近窗口")}
          </Button>
          <Button
            type="button"
            variant="outline"
            disabled={!data || busy}
            onClick={() => {
              if (data)
                inspect({
                  shard: shard.id,
                  startOffset: data.retainedStart,
                  endOffset: data.observedEnd,
                  observedEnd: data.observedEnd,
                  limit: 32,
                });
            }}
          >
            {t("Retained start", "保留起点")}
          </Button>
          <Button
            type="button"
            variant="ghost"
            disabled={!busy}
            onClick={() => {
              void client.cancelQueries({ queryKey, exact: true });
              setCancelled(true);
            }}
          >
            {t("Cancel scan", "取消扫描")}
          </Button>
        </div>
      </form>
      {Boolean(error) && <FailureNotice error={error} compact />}
      {cancelled && (
        <p className="subtle-note" role="status">
          {t(
            "The scan was cancelled. Start another bounded scan when ready.",
            "扫描已取消，可重新发起有界扫描。",
          )}
        </p>
      )}
      <DataTable<Message>
        caption={t("Messages in selected shard", "所选 Shard 的消息")}
        data={data?.items ?? []}
        state={<ReadState loading={busy} error={error} empty={Boolean(data)} />}
        columns={[
          {
            accessorKey: "offset",
            header: "Offset",
            cell: ({ row }) => (
              <button
                className="text-button"
                onClick={() => setSelected(row.original)}
              >
                {row.original.offset}
              </button>
            ),
          },
          { accessorKey: "id", header: "MessageId" },
          { accessorKey: "tier", header: t("Tier", "存储层") },
          {
            accessorKey: "payloadBytes",
            header: t("Payload bytes", "正文字节数"),
            cell: ({ row }) =>
              formatDecimal(row.original.payloadBytes, language),
          },
          {
            id: "truncated",
            header: t("Preview", "预览"),
            cell: ({ row }) =>
              row.original.payloadTruncated || row.original.metadataTruncated
                ? t("Truncated", "已截断")
                : t("Complete", "完整"),
          },
          {
            id: "context",
            header: t("Context", "上下文"),
            cell: ({ row }) => (
              <Button
                size="sm"
                variant="outline"
                onClick={() => context(row.original.offset)}
                disabled={busy}
              >
                {t("Before / after", "查看前后")}
              </Button>
            ),
          },
        ]}
      />
      <div className="table-footer">
        <div>
          <ObservationLabel
            observation={data}
            stale={Boolean(error) && Boolean(data)}
          />
          <p>
            {data
              ? `${t("Scanned", "已扫描")} [${data.scannedStart}, ${data.scannedEnd}) · ${t("Matches", "命中")} ${data.items.length} · ${data.stopReason}`
              : t("No scan result", "尚无扫描结果")}
          </p>
        </div>
        <div className="inline-actions">
          <Button
            size="sm"
            variant="outline"
            disabled={!history.length || busy}
            onClick={() => {
              const previous = history.at(-1);
              setHistory((values) => values.slice(0, -1));
              if (previous) inspect(previous, true);
            }}
          >
            {t("Previous range", "上一范围")}
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={!data?.nextOffset || busy}
            onClick={() => {
              if (data?.nextOffset)
                inspect({
                  ...request,
                  cursor: data.nextCursor ?? undefined,
                  startOffset: data.nextOffset,
                  observedEnd: data.observedEnd,
                });
            }}
          >
            {t("Continue", "继续查找")}
          </Button>
        </div>
      </div>
      {selected && (
        <MessageDetails message={selected} onClose={() => setSelected(null)} />
      )}
    </>
  );
}

function MessageDetails({
  message,
  onClose,
}: {
  message: Message;
  onClose: () => void;
}) {
  const { t } = useLocale();
  const [encoding, setEncoding] = useState("utf8");
  const [metadataEncoding, setMetadataEncoding] = useState("utf8");
  const decode = (value: string, format: string) => {
    try {
      return displayBytes(value, format);
    } catch {
      return t(
        "Cannot decode with this format. Select Base64 or hex.",
        "当前格式无法解码，请选择 Base64 或 hex。",
      );
    }
  };
  return (
    <section
      className="message-preview"
      aria-label={t("Message content", "消息内容")}
    >
      <header>
        <div>
          <h3>{t("Message content", "消息内容")}</h3>
          <p>
            {message.id} · {message.shard} · {message.offset}
          </p>
        </div>
        <Button variant="ghost" onClick={onClose}>
          {t("Close", "关闭")}
        </Button>
      </header>
      <label>
        {t("Payload format", "正文格式")}{" "}
        <select
          value={encoding}
          onChange={(event) => setEncoding(event.target.value)}
        >
          {["utf8", "json", "base64", "hex"].map((item) => (
            <option key={item}>{item}</option>
          ))}
        </select>
      </label>
      {message.payloadTruncated && (
        <p className="notice">
          {t(
            "Payload preview is truncated. The byte count describes the original message.",
            "正文预览已截断，字节数为原消息大小。",
          )}
        </p>
      )}
      <pre>{decode(message.payloadBase64, encoding)}</pre>
      <label>
        {t("Key / property format", "key / 属性格式")}{" "}
        <select
          value={metadataEncoding}
          onChange={(event) => setMetadataEncoding(event.target.value)}
        >
          {["utf8", "base64", "hex"].map((item) => (
            <option key={item}>{item}</option>
          ))}
        </select>
      </label>
      {message.metadataTruncated && (
        <p className="notice">
          {t(
            "Key / property preview is truncated: at most 8 KiB of key, 16 properties and 512 bytes per property. Missing preview entries do not imply missing stored properties.",
            "key / 属性预览已截断：最多展示 8 KiB key、16 个属性，每个属性值最多 512 字节。预览中未展示的属性不代表存储中不存在。",
          )}
        </p>
      )}
      <h4>Key</h4>
      <pre>
        {message.keyBase64 === null
          ? t("Missing", "缺失")
          : decode(message.keyBase64, metadataEncoding)}
      </pre>
      <h4>{t("Properties", "属性")}</h4>
      {Object.entries(message.propertiesBase64).map(([name, value]) => (
        <div key={name}>
          <strong>{name}</strong>
          <pre>{decode(value, metadataEncoding)}</pre>
        </div>
      ))}
      {Object.keys(message.propertiesBase64).length === 0 && (
        <p>{t("No properties in this preview.", "此预览中无属性。")}</p>
      )}
    </section>
  );
}

export function SendMessage({
  topicId,
  topicName,
  onClose,
}: {
  topicId: string;
  topicName: string;
  onClose: () => void;
}) {
  const { t } = useLocale();
  return (
    <WriteForm
      title={t("Send message", "发送消息")}
      target={topicName}
      path={`/api/v1/topics/${topicId}/send`}
      onClose={onClose}
      readBody={(form): SendMessageInput => {
        const encoding = field(form, "encoding");
        const payloadBase64 = toBase64(
          String(form.get("payload") ?? ""),
          encoding,
        );
        if (encoding === "json") JSON.parse(String(form.get("payload") ?? ""));
        const key = String(form.get("key") ?? "");
        const input = {
          payloadBase64,
          keyBase64: form.has("hasKey")
            ? toBase64(key, field(form, "metadataEncoding"))
            : null,
          propertiesBase64: parseProperties(
            field(form, "properties"),
            field(form, "metadataEncoding"),
          ),
        };
        validateMessageBudget(input);
        return input;
      }}
    >
      <label className="form-field">
        <span>{t("Payload format", "正文格式")}</span>
        <select name="encoding">
          <option value="utf8">UTF-8</option>
          <option value="json">JSON</option>
          <option value="base64">Base64</option>
        </select>
      </label>
      <label className="form-field">
        <span>{t("Payload", "正文")}</span>
        <textarea name="payload" rows={7} autoComplete="off" />
      </label>
      <label className="form-field">
        <span>{t("Key / property encoding", "key / 属性编码")}</span>
        <select name="metadataEncoding">
          <option value="utf8">UTF-8</option>
          <option value="base64">Base64</option>
        </select>
      </label>
      <label className="check-label">
        <input type="checkbox" name="hasKey" />
        {t("Include a key (empty values are allowed)", "包含 key（允许空值）")}
      </label>
      <Field name="key" label="Key" />
      <Field
        name="properties"
        label={t("Property object (string values)", "属性对象（值为字符串）")}
        value="{}"
      />
      <p className="footnote">
        {t(
          "Console limits: 1 MiB payload, 256-byte key, up to 64 properties (128-byte names and 16,384 Base64 characters per value), and 1,500,000 bytes for the complete JSON request. The actual Topic limit is unknown and checked by the cluster. No message content is added to the result link.",
          "Console 限制：正文 1 MiB、key 256 字节、最多 64 个属性（名称 128 字节、每值 Base64 最长 16,384 字符），完整 JSON 请求最多 1,500,000 字节。Topic 实际限额未知，由集群校验。消息内容不会进入结果链接。",
        )}
      </p>
    </WriteForm>
  );
}
