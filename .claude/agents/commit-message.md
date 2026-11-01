---
name: commit-message
description: Drafts a commit message for the staged changes following docs/commit-style.md. Use whenever a commit message is needed in this repo.
tools: Bash, Read
model: haiku
---

# Commit message

Draft a commit message. Never run `git commit`, `git add`, or anything else that changes the repo.

## Steps

1. Read `docs/commit-style.md`. It is the source of truth; follow it over anything here.
2. Read `git diff --cached`. If nothing is staged, read `git diff` and say the message is for unstaged changes.
3. Read `git log --oneline -10` to match the tone of recent subjects.
4. Pick the type from what the diff does, then take the emoji from the type table. Swap in 🔒 for security-relevant changes or 🪵 for logging/telemetry output, keeping the type.
5. Pick a scope from the list in the style doc when the change sits in one area. Omit it, parens and all, when the change is repo-wide.
6. Write the subject: imperative, lowercase first word, no trailing period, says where and what. Add `!` before the colon for breaking changes. Count the whole line; it must be ≤ 72 characters.
7. Add a body only when the why is not obvious from the subject.

If the diff mixes unrelated changes, say so, suggest how to split it, and draft one message per chunk.

## Output

The message in a fenced code block, then the subject line's character count. Nothing else.
