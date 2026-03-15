# Harness Engineering — Reference

A reference document capturing the key ideas behind harness engineering for agent-driven software development. This is the conceptual foundation for IonDB's development harness (see `docs/requirements/harness/v0.md` for our specific implementation).

**Primary sources:**

- [OpenAI — Harness Engineering: Leveraging Codex in an Agent-First World](https://openai.com)
- [Fowler/Böckeler — Harness Engineering](https://martinfowler.com)
- [Fowler/Morris — Humans and Agents in Software Engineering Loops](https://martinfowler.com)

---

## What Is Harness Engineering?

Harness engineering is the practice of designing and maintaining the system of context, constraints, feedback loops, and governance that surrounds AI coding agents. The harness is not the code — it is the environment that makes agents produce correct, consistent, architecturally coherent code across thousands of iterations.

The term was named by Mitchell Hashimoto (co-founder of HashiCorp) in early February 2026 and expanded by OpenAI days later in their write-up about building a million-line internal product with zero manually written code.

**The core insight:** the bottleneck is never the agent's ability to write code. It is the lack of structure, tools, and feedback mechanisms surrounding it. When an agent fails, the fix is a harness improvement — not manual intervention on the code.

---

## The Three Pillars

Martin Fowler's analysis (by Birgitta Böckeler) categorizes the harness into three pillars. OpenAI's approach mixes deterministic and LLM-based methods across all three.

### Pillar 1: Context Engineering

The practice of organizing and exposing information so agents can reason over it effectively.

**Key practices:**

- **AGENTS.md / CLAUDE.md as a map, not an encyclopedia.** OpenAI uses a ~100-line file that serves as a table of contents pointing to deeper sources of truth in a structured `docs/` directory. Every token in the agent's context window that isn't relevant to the current task is noise that degrades performance.

- **The repository is the knowledge base.** Documentation, architecture decisions, requirements, and specifications all live as versioned artifacts in the repo. If information isn't discoverable to the agent, it effectively doesn't exist — in the same way it would be unknown to a new hire joining three months later.

- **Onboard agents like teammates.** Give the agent product principles, engineering norms, and team culture the same way you would onboard a new human. This leads to better-aligned output.

- **Context is injected through tooling, not just documents.** Custom linter error messages contain remediation instructions. Test failure output explains the expected invariant. The tools teach the agent while it works.

### Pillar 2: Architectural Constraints

Structural rules enforced mechanically — not through documentation alone.

**Key practices:**

- **Dependency layers.** Code can only depend "forward" through a fixed set of layers. OpenAI uses: Types → Config → Repo → Service → Runtime → UI. Cross-cutting concerns enter through a single explicit interface (Providers). Everything else is disallowed and enforced mechanically.

- **Custom linters with remediation messages.** Because the lints are custom, the error messages can be written specifically for agents. When an agent violates a constraint, the error message tells it exactly how to fix the violation. This is context injection through tooling — the constraint teaches as it enforces.

- **Structural tests.** Tests that validate architectural compliance: dependency direction, naming conventions, file size limits, module structure. These are `cargo test`-style assertions about the shape of the codebase, not the behavior of the code.

- **Taste invariants.** A small set of non-negotiable design rules enforced statically: structured logging, naming conventions for schemas and types, file size limits, platform-specific reliability requirements. These capture human taste once and enforce it continuously on every line of code.

- **Enforce invariants, not implementations.** The constraints define the boundaries; agents have freedom within them. This lets agents ship fast without undermining the architectural foundation.

### Pillar 3: Garbage Collection

Continuous, automated cleanup that fights entropy and architectural drift.

**Key practices:**

- **Recurring sweep agents.** On a regular cadence, background tasks scan for pattern violations, documentation staleness, dead code, test gaps, and inconsistencies. They open targeted cleanup PRs — most reviewable in under a minute.

- **Pay debt continuously, not in bursts.** OpenAI's team initially spent every Friday cleaning up "AI slop." That didn't scale. Continuous small increments beat painful periodic bursts. Technical debt is a high-interest loan.

- **Human taste encoded once, enforced continuously.** A human identifies a quality issue, encodes it as a lint rule or sweep pattern, and every future agent iteration benefits. The harness compounds human judgment over time.

---

## The Loop Model

From Kief Morris at Fowler's site: software engineering with agents operates across nested loops, each with different feedback cycles.

### Inner Loop — Agent Session

The tightest feedback cycle. An agent implements a task, runs quality gates, reads error messages, fixes issues, and iterates. Fully autonomous. Self-correcting via deterministic checks with remediation instructions.

### Middle Loop — PR / Review

A PR is opened. CI runs the full verification stack. Reports are generated. Review happens (by humans, by other agents, or both). Issues are fixed and re-validated.

### Outer Loop — Harness Improvement

The meta-loop. Humans observe patterns in agent failures across many sessions and PRs. They improve the harness: add lints, improve docs, create new structural tests, add sweep patterns. Every agent failure is a harness improvement opportunity. The harness is never "done."

### The Key Distinction: *In the Loop* vs. *On the Loop*

**In the loop:** Fix the code directly or tell the agent to make a correction. This doesn't scale — humans become the bottleneck because agents generate code faster than humans can inspect it.

**On the loop:** Change the harness that produced the code so it produces the correct result next time. This scales — every harness improvement benefits every future agent session.

Quality of outcomes is a function of harness quality, not agent supervision intensity.

---

## The Flywheel

As each engineer encodes their taste and expertise into the repository — through lints, docs, structural tests, sweep patterns, and architectural constraints — every agent on the team gets better. The compound effect is real:

1. Human identifies a quality issue.
2. Human encodes it as a harness component (lint, test, doc, sweep).
3. All future agent sessions benefit from that encoding.
4. Agents produce fewer issues of that type.
5. Human time is freed to identify higher-order quality issues.
6. Repeat.

OpenAI reported scaling from roughly a quarter-engineer-equivalent per person at the start to 3–10 engineers' worth of throughput per person. The harness is the leverage.

---

## What the Harness Is Not

- **Not just an AGENTS.md file.** OpenAI explicitly calls out that what they built is "much more work than just generating and maintaining Markdown rules files." The deterministic tooling (linters, structural tests, sweeps) is the heavy lifting.

- **Not prompt engineering.** The harness includes context engineering, but goes far beyond it. Architectural constraints and garbage collection are equally important pillars.

- **Not a one-time setup.** The harness evolves continuously. Every agent failure is a signal to improve it. The outer loop never terminates.

- **Not a replacement for taste.** Human judgment decides *what* to enforce. The harness automates *how* to enforce it. Engineers become environment designers — their taste is the input, the harness is the output.

---

## Applying to IonDB

For an embedded database engine targeting `no_std` microcontrollers through full Linux systems, the harness gets domain-specific extensions:

- **Cross-tier compilation as a structural test layer.** The `thumbv6m-none-eabi` build is the ultimate architectural constraint — if code compiles for Cortex-M0 with `no_std` and no heap, it respects every memory and dependency rule by construction. This is the IonDB equivalent of dependency-layer enforcement.

- **Feature-flag combinatorics as a constraint surface.** IonDB's feature flags (`no_std`, `alloc`, `acid`, `concurrency`, etc.) create a combinatorial space of valid configurations. The harness must verify that every meaningful combination compiles and passes tests — not just the default profile.

- **Resource budgets as taste invariants.** Memory caps (2 KB RAM for Tier 1), binary size limits (< 32 KB `.text`), and zero-allocation guarantees are non-negotiable design rules. These are captured once in requirements and enforced mechanically via CI scripts that parse linker maps, allocator counters, and binary size reports.

- **Simulator environments as feedback loops.** QEMU, Renode, and Miri extend the inner loop to cross-compiled targets. An agent can write Cortex-M0 code, run it on an emulated target, read the output, and iterate — all without human intervention or physical hardware.

- **Crash simulation as a verification layer.** The `FailpointIoBackend` injects I/O failures at configurable points to exercise every recovery path. WAL replay, partial checkpoint recovery, and corrupted-file detection are validated automatically — agents get deterministic feedback on durability correctness.

- **Dogfood applications as integration anchors.** The `sensor-log`, `edge-config`, and `fleet-telemetry` apps are living integration tests. If a library change breaks a dogfood app, the PR is blocked. This catches API regressions, performance regressions, and feature-flag conflicts at the harness level.

- **Trait-based pluggability as an architectural constraint.** `StorageEngine`, `MemoryAllocator`, `IoBackend`, and `Codec` are the architectural seams. The harness enforces that all implementations go through these traits — no backdoor coupling between crates. Dependency direction is validated structurally: `iondb-core` depends on nothing; implementation crates depend only on `iondb-core`.

See `docs/requirements/harness/v0.md` for the full specification.
