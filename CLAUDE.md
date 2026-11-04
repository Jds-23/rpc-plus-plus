# rpc-plus-plus

## Checks

Run checks through `just` (same recipes CI uses): `just ci` before handing work back, `just test <filter>` or `just test-file <name>` for targeted runs. `just` lists all recipes.

## Writing

Prose (docs, READMEs, RFCs, PR descriptions, commit bodies) follows two project skills in `.claude/skills/`. Both are user-invoked only; suggest them, don't assume they ran.

- `/technical-writing`: Diátaxis mode, Google developer style, STE, Global English. Use when writing or reviewing docs.
- `/unslop`: cuts AI tells. Rule numbers are stable ids; `technical-writing` cites rule 26 (abstract metaphor nouns). Propose additions to it as a diff in the reply, never edit the skill directly.

## Commits

One line, emoji first, Conventional Commits underneath:

```
<emoji> <type>(<scope>): <subject>
```

Imperative mood, lowercase after the colon, no trailing period, ≤ 72 chars. Scope is optional and comes from `src/` (`app`, `config`, `decider`, `http`, `jsonrpc`, `observer`, `proxy`, `telemetry`, `upstream`) plus `tests`, `docs`, `deps`.

Emoji is derived from the type: ✨ `feat`, 🐛 `fix`, 🧹 `refactor`, ⚡ `perf`, 🏗️ `test`, 📝 `docs`, 🎨 `style`, 🔧 `chore`, 📦 `build`, ⏪ `revert`. Two overrides swap the emoji but keep the type: 🔒 for security-relevant changes, 🪵 for logging/telemetry.

Full rules and examples: [docs/commit-style.md](docs/commit-style.md).

## Agents

Project subagents live in `.claude/agents/`.

- `commit-message`: drafts a commit message for staged changes per `docs/commit-style.md`. Never commits.
- `comment-sicko`: hunts comments in given files or the diff against `main` (`/no-comments`). Keeps only license headers, external-quirk notes, faulty-lint suppressions, and `pub` API doc comments.
