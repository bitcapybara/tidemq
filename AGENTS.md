# AGENTS.md

These rules apply throughout the repository.

1. **Keep the task handoff current.** Read and follow the `task-handoff-memory` skill throughout each task. Consult it when starting or resuming work and before design, implementation, or testing. Update the task ledger and evidence at each checkpoint and before pausing, handing off, or closing the task.
2. **Pass CI before merging into main.** Run all applicable tests and gates on the exact candidate revision. Investigate intermittent failures and verify fixes; a successful rerun alone does not establish a root cause.
3. **Keep documentation and reusable tools current.** Update the README and relevant docs when behavior or workflows change. Consider checking in reusable scripts and configuration with usage instructions.
4. **Support the current version.** TideMQ is under active development. Update protocols, interfaces, configuration, and callers together, and remove obsolete compatibility paths.
5. **Parallelize complex work.** Where practical, divide work among agents in separate worktrees with clear ownership and dependencies. Integrate and verify their results together.
6. **Clean up task resources.** Verify ownership and preserve source and evidence before removing task-created processes, temporary files, containers, cluster resources, or worktrees.
7. **Require independent review before merging into main.** An independent agent must review the candidate and all in-scope findings must be resolved. Use a fresh agent for every review. State a fixed baseline, normally the merge base with the target branch, and review the complete diff from that baseline, including relevant working-tree changes.
8. **Put correctness and observability first.** For new behavior, verify logs, traces, and metrics cover critical paths and failures. Cover the behavior with applicable unit, integration, and automated end-to-end tests in CI.
9. **Get explicit approval before starting an iterative write-review cycle.** Once approved, run dependent tests or CI only after the independent reviewer closes the review with no unresolved in-scope findings. After every change, use a fresh reviewer and the same baseline before resuming those checks. Bind every review and validation result to the exact diff or commit it covered.
10. **Triage failures before changing code or rerunning CI.** Preserve the first failing step and its evidence. Fix attributable failures, obtain a fresh independent review, and then rerun the applicable checks. If evidence shows a failure is unrelated, record the diagnosis and wait for user direction. If attribution is uncertain, investigate and report the uncertainty.
