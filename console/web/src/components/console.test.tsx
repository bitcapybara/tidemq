import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import { ApiError } from "@/api/client";
import { progressObservationSchema } from "@/api/types";
import { normalizeTimes } from "@/api/client";
import { FailureNotice, ProgressObservationLabel } from "./console";

it("distinguishes expired message offsets from expired operation recovery", () => {
  const offset = renderToStaticMarkup(
    <FailureNotice error={new ApiError("invalid", "offset_expired", 410)} />,
  );
  const operation = renderToStaticMarkup(
    <FailureNotice error={new ApiError("invalid", "recovery_expired", 410)} />,
  );
  expect(offset).toContain("outside the retained range");
  expect(offset).toContain("Refresh the shard boundaries");
  expect(operation).toContain("recovery window has expired");
  expect(operation).toContain("Do not replay this action automatically");
  expect(operation).not.toContain("select another offset");
});

it("labels partial missing progress as unknown without borrowing a metadata timestamp or zero", () => {
  const observation = progressObservationSchema.parse(
    normalizeTimes({
      source: "perShardLinearizableGroupCursorSamples",
      observedFromMs: null,
      observedUntilMs: null,
      sampledShards: null,
      partial: true,
      reason: "observationBudgetExhausted",
    }),
  );
  const html = renderToStaticMarkup(
    <ProgressObservationLabel observation={observation} />,
  );
  expect(html).toContain("No sample time provided");
  expect(html).toContain("Unknown");
  expect(html).toContain("Partial");
  expect(html).toContain("time budget was exhausted");
  expect(html).toContain("not an atomic snapshot");
  expect(html).not.toContain("1970");
  expect(html).not.toContain("metadataLinearizable");
});

it("shows real progress ranges and distinguishes a known empty sample count from unknown", () => {
  const observation = progressObservationSchema.parse(
    normalizeTimes({
      source: "perShardLinearizableGroupCursorSamples",
      observedFromMs: "1700000000000",
      observedUntilMs: "1700000000123",
      sampledShards: "2",
      partial: false,
      reason: null,
    }),
  );
  const html = renderToStaticMarkup(
    <ProgressObservationLabel observation={observation} stale />,
  );
  expect(html).toContain("Stale sample range");
  expect(html).toContain(observation.source);
  expect(html).toContain("2023");
  expect(html).toContain("Sampled Shards: 2");
  expect(html).not.toContain("Partial");
  const empty = renderToStaticMarkup(
    <ProgressObservationLabel
      observation={{
        ...observation,
        observedFromMs: null,
        observedUntilMs: null,
        observedFrom: null,
        observedUntil: null,
        sampledShards: "0",
      }}
    />,
  );
  expect(empty).toContain("Sampled Shards: 0");
  expect(empty).toContain("No sample time provided");
  expect(empty).not.toContain("Unknown");
});
