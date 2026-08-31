# AGENTS.md

## Technical Writing Style

- Apply these rules to user-facing technical explanations, reviews, reports, design
  documents, and local notes.
- Write technical material in a neutral, direct, academic, and engineering-oriented
  style.
- Keep observed source facts, derived conclusions, design proposals, and validation
  items explicitly separated.
- Prefer declarative descriptions of mechanisms, interfaces, constraints, and effects.
  Avoid self-referential commentary, defensive disclaimers, rhetorical negation, and
  repeated caveats.
- Qualify a statement only when the available evidence is incomplete. State the exact
  missing evidence and its engineering impact once, then continue with the analysis.
- Express recommendations through scope, priority, tradeoffs, and acceptance criteria.
  Do not dramatize risks or frame ordinary engineering uncertainty as self-doubt.
- Keep terminology stable. Define ambiguous terms once and use the same term for the
  same concept throughout a document.

## `docs/` Directory Changes

- Before modifying files under `docs/`, read `docs/AGENTS.md`.

## Pull Request Guidelines

- Follow `CONTRIBUTING.md` for PR title prefixes, RFC expectations, and
  contribution workflow.
- Before opening a PR for nontrivial work, check whether an existing issue or
  open PR already covers the same change. If the work overlaps, explain the
  difference instead of duplicating it.
- Do not open low-value busywork PRs for isolated typo, style, or mechanical
  changes unless they are part of a substantive requested change.
- Use `.github/pull_request_template.md` when preparing a PR, and fill in the
  relevant sections for description, module, type of change, testing,
  checklist, and AI assistance disclosure.
- For AI-assisted changes, make sure the human submitter has reviewed every
  changed line and can defend the change end-to-end.
- Run pre-commit locally on the files touched by the change before handoff when
  the toolchain is available. If broader hooks or `pre-commit run --all-files`
  rewrite unrelated files, do not include those unrelated edits in the PR.
- Keep PRs lean: review `git diff` before staging, and include only changes
  required for the requested task.
