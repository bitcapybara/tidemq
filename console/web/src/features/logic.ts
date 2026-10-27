import type { SendMessageInput } from "@/api/types";

export function isOffset(value: string): boolean {
  return (
    value.length <= 20 &&
    /^(0|[1-9]\d*)$/.test(value) &&
    BigInt(value) <= 18_446_744_073_709_551_615n
  );
}

export function operationId(
  now = Date.now(),
  random = crypto.getRandomValues(new Uint8Array(16)),
): string {
  if (
    !Number.isSafeInteger(now) ||
    now < 0 ||
    now > 0xffffffffffff ||
    random.byteLength !== 16
  )
    throw new Error("Invalid operation identity input");
  const data = new Uint8Array(random);
  const view = new DataView(data.buffer);
  view.setUint32(0, Math.floor(now / 65536));
  view.setUint16(4, now % 65536);
  view.setUint8(6, (view.getUint8(6) & 0x0f) | 0x70);
  view.setUint8(8, (view.getUint8(8) & 0x3f) | 0x80);
  const hex = Array.from(data, (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function bytesFromBase64(value: string): Uint8Array {
  if (
    value.length % 4 !== 0 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
      value,
    )
  )
    throw new Error("Invalid Base64");
  const decoded = atob(value);
  if (btoa(decoded) !== value) throw new Error("Noncanonical Base64");
  return Uint8Array.from(decoded, (character) => character.charCodeAt(0));
}
export function toBase64(value: string, encoding: string): string {
  if (encoding === "base64") {
    bytesFromBase64(value);
    return value;
  }
  return btoa(
    Array.from(new TextEncoder().encode(value), (byte) =>
      String.fromCharCode(byte),
    ).join(""),
  );
}
export function displayBytes(value: string, encoding: string): string {
  const bytes = bytesFromBase64(value);
  if (encoding === "base64") return value;
  if (encoding === "hex")
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
      " ",
    );
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  return encoding === "json" ? JSON.stringify(JSON.parse(text), null, 2) : text;
}
export function parseProperties(
  value: string,
  encoding: string,
): Record<string, string> {
  const parsed: unknown = JSON.parse(value || "{}");
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
    throw new Error("Expected a property object");
  const result: Record<string, string> = Object.create(null);
  for (const [key, field] of Object.entries(parsed)) {
    if (typeof field !== "string")
      throw new Error("Property values must be strings");
    result[key] = toBase64(field, encoding);
  }
  return result;
}

export function validateMessageBudget(input: SendMessageInput): void {
  const properties = Object.entries(input.propertiesBase64);
  const encoder = new TextEncoder();
  if (
    bytesFromBase64(input.payloadBase64).byteLength > 1_048_576 ||
    (input.keyBase64 !== null &&
      bytesFromBase64(input.keyBase64).byteLength > 256) ||
    properties.length > 64 ||
    properties.some(
      ([name, value]) =>
        encoder.encode(name).byteLength > 128 || value.length > 16_384,
    ) ||
    encoder.encode(
      JSON.stringify({
        ...input,
        operationId: "00000000-0000-7000-8000-000000000000",
      }),
    ).byteLength > 1_500_000
  )
    throw new Error("Message exceeds Console request limits");
}

export function boundedWindow(end: string, floor: string, width = 100n) {
  if (
    !isOffset(end) ||
    !isOffset(floor) ||
    width <= 0n ||
    BigInt(floor) > BigInt(end)
  )
    return null;
  const lower = BigInt(end) - width;
  return {
    start: (lower < BigInt(floor) ? BigInt(floor) : lower).toString(),
    end,
  };
}

export const RESOURCE_PERMISSIONS = [
  "produce", "consume", "inspect", "inspect_message_content", "manage_topics",
];

export function resourceGrantPermissions(permissions: readonly string[]) {
  return permissions.filter((permission) => RESOURCE_PERMISSIONS.includes(permission));
}

export function permissionPreview(
  direct: readonly string[],
  inherited: readonly string[],
  proposed: readonly string[],
) {
  return {
    added: proposed.filter((permission) => !direct.includes(permission)),
    removed: direct.filter((permission) => !proposed.includes(permission)),
    effective: [...new Set([...inherited, ...proposed])].sort(),
    stillInherited: direct.filter(
      (permission) =>
        !proposed.includes(permission) && inherited.includes(permission),
    ),
  };
}

export function operationPath(id: string): string | null {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    id,
  )
    ? `/operations/${id.toLowerCase()}`
    : null;
}

export function componentPath(id: string): string {
  const group = /^data-group:((?:0|[1-9]\d*))$/.exec(id)?.at(1);
  return group && isOffset(group)
    ? `/cluster?componentId=storage&groupId=${group}`
    : `/cluster?componentId=${encodeURIComponent(id)}`;
}

export function formatDecimal(
  value: string | null | undefined,
  locale: string,
): string {
  return value !== null && value !== undefined && /^\d+$/.test(value)
    ? new Intl.NumberFormat(locale).format(BigInt(value))
    : "—";
}
