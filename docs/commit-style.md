# Commit style

Gitmoji fused with Conventional Commits. One line, emoji first.

```
<emoji> <type>(<scope>): <subject>
```

## Rules

- Emoji first, exactly **one** space after it.
- `type` comes from the table below. The emoji is derived from the type — there is no per-commit decision to make.
- `scope` is optional. Omit it, parens and all, when the change is repo-wide. Lowercase.
- Subject: imperative mood (`add`, not `added`/`adds`), lowercase first word, no trailing period.
- Whole subject line ≤ 72 characters.
- Breaking change: `!` before the colon — `✨ feat(rpc)!: drop legacy upstream config`.
- Body is optional and rare — only when the *why* isn't obvious from the subject. Blank line, then prose.

## Type ↔ emoji

| Emoji | Type | Use for |
|---|---|---|
| ✨ | `feat` | new user-visible behaviour |
| 🐛 | `fix` | bug fix |
| 🧹 | `refactor` | restructure, no behaviour change |
| ⚡ | `perf` | measurable speedup |
| 🏗️ | `test` | tests only |
| 📝 | `docs` | docs, spec, design notes |
| 🎨 | `style` | clippy, rustfmt, lint-only |
| 🔧 | `chore` | tooling, config, housekeeping |
| 📦 | `build` | Cargo.toml/lock, deps |
| ⏪ | `revert` | revert a prior commit |

Two sanctioned overrides — swap the emoji, keep the type:

| Emoji | Replaces | When |
|---|---|---|
| 🔒 | ✨/🐛/🧹 | security-relevant: key handling, secret leaks, authz |
| 🪵 | 🔧/✨ | logging, tracing, telemetry output |

## Scopes

From `src/`: `app`, `config`, `decider`, `http`, `observer`, `proxy`, `telemetry`, `upstream`.
Plus `tests`, `docs`, `deps`.

## Before / after

Drawn from this repo's own history, which was rewritten to match:

| Was | Is |
|---|---|
| `remove exclude` | `📝 docs(design): drop upstream exclusion from decider contract` |
| `fix cofiguration -> settings` | `🧹 refactor(settings): load settings.yaml, not configuration.yaml` |
| `🧹 refactor` | `🧹 refactor(startup): extract build_handlers out of main` |
| `clippy ✨` | `🎨 style: fix clippy lints in round robin handler` |
| `🪵 better logs` | `🪵 chore(telemetry): emit structured per-attempt request logs` |

The old subjects all failed the same way: they said *what kind* of change it was but never *where* or *what*.

## Template

`.gitmessage` at the repo root is wired in via `commit.template`, so `git commit` opens with the rules in front of you. If you cloned fresh, re-wire it with:

```sh
git config --local commit.template .gitmessage
```
