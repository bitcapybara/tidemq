import { describe, expect, it } from "vitest";
import {
  boundedWindow,
  formatDecimal,
  isOffset,
  operationPath,
  permissionPreview,
  operationId,
  bytesFromBase64,
  displayBytes,
  parseProperties,
  toBase64,
  componentPath,
  validateMessageBudget,
} from "./logic";
import { resourceIdSchema } from "@/api/types";

describe("offset browsing", () => {
  it("preserves precision and clamps the latest window to the retained floor", () => {
    expect(
      boundedWindow("18446744073709551615", "18446744073709551610"),
    ).toEqual({
      start: "18446744073709551610",
      end: "18446744073709551615",
    });
    expect(boundedWindow("50", "0")).toEqual({ start: "0", end: "50" });
    expect(boundedWindow("5", "6")).toBeNull();
    expect(isOffset("18446744073709551616")).toBe(false);
    expect(isOffset("1e3")).toBe(false);
    expect(isOffset("-1")).toBe(false);
    expect(formatDecimal("9007199254740993", "en")).toBe(
      "9,007,199,254,740,993",
    );
  });
});

describe("stable identities and byte previews", () => {
  it("creates a timestamp-bearing UUID v7 with an RFC variant and keeps its random suffix", () => {
    const first = operationId(1700000000000, new Uint8Array(16));
    const second = operationId(1700000000000, new Uint8Array(16).fill(255));
    expect(first).toBe("018bcfe5-6800-7000-8000-000000000000");
    expect(second).toMatch(/^018bcfe5-6800-7fff-bfff-/);
    expect(first).not.toBe(second);
    expect(() => operationId(-1)).toThrow();
  });
  it("preserves empty values and arbitrary bytes without rendering them as markup", () => {
    expect(toBase64("<script>文本</script>", "utf8")).toBe(
      "PHNjcmlwdD7mlofmnKw8L3NjcmlwdD4=",
    );
    expect(displayBytes("/wA=", "hex")).toBe("ff 00");
    expect(() => displayBytes("/wA=", "utf8")).toThrow();
    expect(bytesFromBase64("")).toEqual(new Uint8Array());
    expect(() => bytesFromBase64("Zh==")).toThrow();
    expect(() => bytesFromBase64("AA")).toThrow();
    const properties = parseProperties(
      '{"__proto__":"","empty":"","text":"a"}',
      "utf8",
    );
    expect(Object.getPrototypeOf(properties)).toBeNull();
    expect(Object.hasOwn(properties, "__proto__")).toBe(true);
    expect(properties.empty).toBe("");
    expect(properties.text).toBe("YQ==");
    expect(() => parseProperties('{"bad":1}', "utf8")).toThrow();
  });
});

describe("permission preview", () => {
  it("does not imply that removing a direct grant removes inherited access", () => {
    expect(
      permissionPreview(
        ["Consume", "Produce"],
        ["Consume"],
        ["InspectMessage"],
      ),
    ).toEqual({
      added: ["InspectMessage"],
      removed: ["Consume", "Produce"],
      effective: ["Consume", "InspectMessage"],
      stillInherited: ["Consume"],
    });
  });
});

it("accepts only nonsecret operation identifiers in result links", () => {
  const id = "018bcfe5-6800-7000-8000-000000000000";
  expect(operationPath(id)).toBe(`/operations/${id}`);
  expect(operationPath("tmq_test_token_value")).toBeNull();
  expect(operationPath("../token?secret=x")).toBeNull();
});

it("accepts core hash-derived resource IDs without treating them as operation UUIDs", () => {
  const shard = "ca92ffb9-8753-86fd-f214-0587279210a3";
  expect(resourceIdSchema.safeParse(shard).success).toBe(true);
  expect(operationPath(shard)).toBeNull();
  expect(resourceIdSchema.safeParse(`${shard}/../secret`).success).toBe(false);
  expect(resourceIdSchema.safeParse("not-a-resource").success).toBe(false);
});

it("preserves data-group context when opening a consumer's storage component", () => {
  expect(componentPath("data-group:2")).toBe(
    "/cluster?componentId=storage&groupId=2",
  );
  expect(componentPath("metadata")).toBe("/cluster?componentId=metadata");
  expect(componentPath("data-group:2&nodeId=3")).toBe(
    "/cluster?componentId=data-group%3A2%26nodeId%3D3",
  );
});

it("enforces decoded message and UTF-8 metadata budgets before preserving a write", () => {
  const input = {
    payloadBase64: toBase64("x".repeat(1_048_576), "utf8"),
    keyBase64: null,
    propertiesBase64: {},
  };
  expect(() => validateMessageBudget(input)).not.toThrow();
  expect(() =>
    validateMessageBudget({
      ...input,
      payloadBase64: toBase64("x".repeat(1_048_577), "utf8"),
    }),
  ).toThrow();
  expect(() =>
    validateMessageBudget({
      ...input,
      keyBase64: toBase64("文".repeat(86), "utf8"),
    }),
  ).toThrow();
  expect(() =>
    validateMessageBudget({
      ...input,
      propertiesBase64: { ["文".repeat(43)]: "" },
    }),
  ).toThrow();
  const properties = Object.fromEntries(
    Array.from({ length: 16 }, (_, index) => [
      `field${index}`,
      "eHh4".repeat(4096),
    ]),
  );
  expect(() =>
    validateMessageBudget({ ...input, propertiesBase64: properties }),
  ).toThrow();
});
