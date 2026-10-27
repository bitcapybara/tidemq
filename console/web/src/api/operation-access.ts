import type { LocalOperation } from "./operations";

export function redactDeniedOperations(
  entries: Record<string, LocalOperation>,
  denied: ReadonlySet<string>,
): Record<string, LocalOperation> {
  let redacted = entries;
  for (const [id, entry] of Object.entries(entries)) {
    if (
      !denied.has(`operation:${id}`) ||
      (entry.phase === "recover" &&
        entry.result === undefined &&
        entry.secret === undefined &&
        entry.error === undefined &&
        entry.resourceTo === undefined)
    )
      continue;
    redacted = {
      ...redacted,
      [id]: { phase: "recover", returnTo: entry.returnTo, epoch: entry.epoch },
    };
  }
  return redacted;
}
