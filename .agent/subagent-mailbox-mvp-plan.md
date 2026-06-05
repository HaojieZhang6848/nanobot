# Subagent Mailbox MVP Plan

This document records the agreed implementation direction for replacing the
current subagent wait behavior. It is intentionally narrower than PR #3461.

## Goal Prompt

Use this goal when asking an agent to implement the change:

```text
Implement a minimal, mergeable mailbox-backed manager-worker coordination MVP
that replaces the current implicit subagent-result path through
pending_queue/mid-turn injection.

Goals:
1. Borrow only the core ideas from PR #3461: manager-worker mailbox mechanics,
   task/result messages, and explicit poll/wait. Treat task-scoped session state
   as an optional supporting idea, not a required first-pass feature. Do not copy the
   broadcast/bid/aggregation/circuit-breaker/create-instance skill pieces.
2. Ensure subagent/task results no longer masquerade as ordinary inbound
   messages routed through the main session pending_queue, and ensure
   _drain_pending does not block the current turn just because a subagent is
   still running.
3. Add a clear mailbox protocol layer with minimal dispatch/spawn, result,
   poll/wait, cancel/finalize semantics. This is a strict manager-worker model,
   not peer-to-peer agent collaboration.
4. Keep responsibilities separated: AgentLoop handles user turn scheduling,
   AgentRunner handles model/tool execution, and mailbox/worker management
   handles manager-worker messages and task lifecycle.
5. Choose the smallest deterministic implementation that can be tested cleanly.
   In-process and filesystem-backed stores are both acceptable design options;
   if filesystem storage is used, atomic writes, result deduplication, and
   restart-safe reads must be covered.
6. Preserve existing chat behavior: ordinary user follow-up messages,
   streaming stream_end, /stop, session history, and long_task goal state must
   keep working.
7. Add focused tests covering: dispatch does not block the current turn,
   explicit result wait/poll, user follow-up is not confused with worker result,
   cancellation/finalization semantics if implemented, result deduplication, and
   removal of the old implicit subagent wait path.
8. Run the relevant pytest and ruff checks. If any cannot run, report the reason
   and residual risk.

Non-goals:
- Do not implement a full multi-process agent network.
- Do not implement decentralized P2P agent collaboration. Prior P2P-style agent
  exchange has already proven unsuitable for the current agent behavior.
- Do not implement broadcast/bid marketplace behavior.
- Do not implement create-instance skill.
- Do not do broad WebUI work. Any status-event adaptation must be minimal and
  justified by correctness or test evidence.

Done means:
The main agent can dispatch a background worker and finish the current turn
normally. The worker result lands in a mailbox/result store. The main agent can
consume it through an explicit wait/poll tool. User messages continue through
the normal user-turn/pending-message path and are not blocked by hidden subagent
waiting.
```

## Problem To Solve

The current implementation couples subagent completion to the main agent's
mid-turn injection queue:

- Subagent completion is published as a synthetic inbound message.
- The main loop routes same-session inbound messages to the active pending queue.
- The runner drains that queue as mid-turn injections.
- If the queue is empty but a subagent is still running, the drain callback can
  wait for the queue for a long time.

This makes subagent lifecycle a hidden dependency of the current turn. It also
mixes user follow-up messages with worker results in the same queue.

The target behavior is explicit message passing:

- User messages and worker results have separate paths.
- Worker results are stored as task results, not injected as ordinary inbound
  messages by default.
- Waiting is an explicit tool action, not a hidden AgentLoop behavior.

## Current Code Points

These line references were taken from the origin/main-based worktree at the time
this plan was written. They are landmarks, not a requirement to preserve exact
line numbers.

- `nanobot/agent/loop.py:712` defines `_drain_pending`.
- `nanobot/agent/loop.py:733-738` drains pending messages with `get_nowait`.
- `nanobot/agent/loop.py:743-747` blocks when no pending items exist but the
  session still has running subagents.
- `nanobot/agent/loop.py:880-896` routes same-session inbound messages into the
  active pending queue.
- `nanobot/agent/subagent.py:174-198` creates a background task for a subagent.
- `nanobot/agent/subagent.py:242-257` runs the subagent using `AgentRunner`.
- `nanobot/agent/subagent.py:309-330` publishes the subagent result back as an
  inbound message.
- `nanobot/agent/tools/spawn.py:71-88` exposes the current spawn tool.

## Consensus Direction

Use a small mailbox-backed coordination layer. Do not move directly to a full
Codex-style thread tree. Do not keep fixing the hidden pending queue wait as the
long-term architecture.

This is explicitly a master/worker model:

- The main agent is the only orchestrator for the user-facing task.
- Workers receive bounded delegated tasks and return results.
- Workers do not negotiate with each other, bid on work, form a decentralized
  network, or independently decide to report to the user.
- Any further delegation must be a deliberate future design, not an accidental
  property of the mailbox protocol.

The key property is the protocol boundary, not the storage backend. Backend
choice is an implementation decision: choose the smallest option that proves the
manager-worker behavior and keeps tests deterministic.

Possible concepts. These names are examples, not settled API:

- `TaskId`: stable id returned by dispatch/spawn.
- `WorkerId`: logical worker id. For the first version this can be in-process
  worker ids owned by the main agent.
- `TaskMessage`: request payload with task description, origin session, created
  time, deadline, and cancellation metadata.
- `TaskResult`: completion payload with task id, status, content, error, sender,
  completed time, and dedupe key.
- `MailboxStore`: append/read/claim result records.
- `WorkerManager`: starts in-process workers and writes results to the mailbox.
- `wait_subagents` or `wait_agent`: explicit tool that consumes mailbox results.
- `poll_subagents` or `poll_agent`: non-blocking status/result check.
- `cancel_subagent` or `finalize_task`: explicit cancellation/finalization.

## Borrow From PR #3461

The useful ideas from PR #3461 are:

- Filesystem-backed inbox/processed layout as a possible persistence model.
- Dispatch writes a task message and returns immediately.
- Result reporting writes a separate result message to the requester.
- Polling is explicit.
- Task-scoped sessions may give delegated work isolated context.

Borrow only mailbox mechanics, not P2P semantics. PR #3461's broader
decentralized collaboration direction is not a good fit for the current agent.
The MVP should preserve a clear main-agent-to-worker hierarchy.

Specific PR #3461 landmarks:

- `nanobot/p2p/shell.py:18` defines a mailbox-like shell.
- `nanobot/p2p/shell.py:21-29` gives each agent inbox and processed dirs.
- `nanobot/p2p/shell.py:66-122` dispatches a task by writing to target inbox.
- `nanobot/p2p/shell.py:124-158` polls task status/results.
- `nanobot/p2p/shell.py:259-283` writes result messages.
- `nanobot/session/manager.py:584-612` sketches task-scoped sessions.
- `nanobot/agent/context.py:77-97` adds task-session collaboration hints.

Do not borrow these parts for the MVP:

- Peer-to-peer/decentralized agent exchange.
- Broadcast/bid aggregation.
- Circuit breaker/failover.
- Create-instance skill.
- Heartbeat-based inbox scanning.
- Default-channel `report_user` delivery without reliable callback metadata.

## Architecture Boundary

Keep these responsibilities separate:

- `AgentLoop`: owns user turn scheduling, session locks, user pending messages,
  commands, streaming callbacks, and runtime events.
- `AgentRunner`: owns model/tool iteration and injection callback execution.
- `MailboxStore`: owns task/result records and deduplication.
- `WorkerManager`: owns worker lifecycle, cancellation, and result publication.
- Tools: expose explicit operations to the LLM: spawn/dispatch, wait/poll,
  cancel/finalize.

Workers should not have tools that let them directly orchestrate peer workers in
the first version. They may use ordinary task tools to complete their delegated
work, then return a result to the main agent.

The mailbox layer should not know about WebUI-specific wire details. If UI status
is needed, emit generic runtime events or expose status through existing session
state patterns.

## Implementation Shape

Suggested first pass. This is a starting shape, not a fixed design:

1. Add a small mailbox module, for example `nanobot/agent/mailbox.py` or
   `nanobot/session/mailbox.py`.
2. Add dataclasses for task request/result/status. Keep them JSON-serializable.
3. Choose a simple store backend. In-memory is fine for a first implementation;
   filesystem is fine only if it stays simple and is tested for atomicity and
   deduplication.
4. Modify `SubagentManager` into a worker supervisor that writes completion to
   the mailbox instead of publishing inbound results.
5. Remove the long subagent-running wait from `_drain_pending`.
6. Add explicit wait/poll/cancel tools.
7. Decide whether to keep the existing `spawn` tool name for compatibility or
   introduce a clearer worker-specific name.
8. Add focused tests before broad refactors.

Open transitional behaviors:

- A worker completion notification may be useful when no active turn exists, but
  this is not part of the agreed MVP unless explicitly chosen. It must not
  reintroduce hidden waiting in `_drain_pending`.
- Session history persistence for worker results needs a deliberate choice:
  write after explicit wait/poll, and decide whether the durable entry is
  assistant, system, or metadata-only.

## Open Decisions

These points are not yet consensus and should not be treated as requirements:

- Store backend: in-memory first, filesystem first, or a small interface with one
  concrete implementation.
- Task-scoped sessions: useful idea from PR #3461, but optional for the MVP.
- Public tool names: keep `spawn`, add `wait_subagents`, use `dispatch_task`, or
  choose clearer worker-specific names.
- Worker completion notification: explicit wait/poll only, or a minimal
  notification when no active turn exists.
- Session history semantics: when and how consumed worker results become durable
  conversation history.
- UI/runtime status: no broad WebUI work; any minimal status event needs a clear
  correctness reason.
- Exact module/class names: `MailboxStore`, `WorkerManager`, and `WorkerId` are
  placeholders for the implementation discussion.

## Test Plan

Minimum tests:

- Dispatch returns before worker completion.
- Current turn reaches final response/stream_end while worker is still running.
- Worker completion is stored in mailbox.
- Explicit wait returns the result once and does not duplicate it.
- Explicit poll reports running/completed/not_found states.
- User follow-up during an active main turn still uses ordinary pending queue
  behavior.
- User follow-up is not ordered behind hidden subagent waits.
- `/stop` cancels active workers for the session.
- Finalize/cancel marks task state and prevents later result injection.
- Existing long_task goal state continuation still works.

Useful regression target:

- A test should fail on the old implementation because `_drain_pending` waits on
  `pending_queue.get()` solely due to a running subagent, then pass after the
  hidden wait is removed.

## Risks And Guardrails

Main risks:

- Accidentally replacing one hidden queue with another hidden queue.
- Accidentally recreating PR #3461's P2P agent network instead of a strict
  manager-worker boundary.
- Duplicating results after repeated wait/poll calls; if persistent storage is
  chosen, duplicating results after restart.
- Losing compatibility with existing spawn tool expectations.
- Making `AgentLoop` larger instead of reducing its subagent-specific knowledge.
- Over-scoping the first PR with marketplace or multi-process behavior.

Guardrails:

- Keep the MVP small.
- Keep the main agent in charge of orchestration.
- Keep waiting explicit.
- Keep user messages and worker results on separate paths.
- Avoid new WebUI behavior unless needed for correctness.
- Preserve existing tests for pending messages, streaming, stop, session history,
  and long_task.

## Review Checklist

Before considering the implementation complete:

- `_drain_pending` no longer blocks for running subagents.
- Subagent/worker result publication does not call `bus.publish_inbound` as the
  primary result path.
- There is a clear task id in every spawn/dispatch response.
- There is a clear explicit way to wait or poll for a task result.
- Repeated wait/poll calls do not duplicate consumed results.
- Cancellation has a defined state.
- The implementation does not include PR #3461 broadcast/bid/create-instance
  features.
- Tests cover the old stuck-turn behavior and the new explicit mailbox behavior.
