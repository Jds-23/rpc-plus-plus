# rpc-plus-plus

## Commits

One line, emoji first, Conventional Commits underneath:

```
<emoji> <type>(<scope>): <subject>
```

Imperative mood, lowercase after the colon, no trailing period, ≤ 72 chars. Scope is optional and comes from `src/` (`rpc`, `route`, `decider`, `settings`, `telemetry`, `handler`, `healthz`, `startup`) plus `tests`, `docs`, `deps`.

Emoji is derived from the type: ✨ `feat`, 🐛 `fix`, 🧹 `refactor`, ⚡ `perf`, 🏗️ `test`, 📝 `docs`, 🎨 `style`, 🔧 `chore`, 📦 `build`, ⏪ `revert`. Two overrides swap the emoji but keep the type: 🔒 for security-relevant changes, 🪵 for logging/telemetry.

Full rules and examples: [docs/commit-style.md](docs/commit-style.md).
