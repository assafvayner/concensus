# concensus

A paxos implementation in rust.

## Code Standards

These rules apply to ALL code written or modified in this repo:

### Style

- NO trivial comments — do not add comments that restate what the code does
- Descriptive variable and function names
- No wildcard imports (e.g., `use foo::*`)
- All imports are at the top of the file or top of module
- Latest stable Rust features are allowed

### Error Handling

- Use `Result<T, E>` with explicit error handling — never panic
- Define custom error types using `thiserror` for domain-specific errors
- Provide helpful, actionable error messages

### Performance

- Be mindful of allocations in hot paths
- Prefer structured logging (tracing/log macros with fields, not string formatting)

### Testing

TODO: fill in when we have tests

### Formatting and Linting

- Format: `cargo +nightly fmt`
- Lint: `cargo clippy -- -D warnings`
- ALWAYS run both after making changes — do not skip this step

## Feature Development

Before writing any code:

1. **Branch:** Confirm you are on a feature branch, not `main`. If on `main`, create a branch named `<username>/<short-description>`.
2. **Plan:** Write an implementation plan that includes testing strategy (unit tests, integration tests, manual verification steps). Add this plan as a comment on the PR.

## Code Review

When reviewing a pull request, follow these rules:

### Tone

- Collegiate and constructive — write as a peer, not an authority
- Use phrases like "consider...", "what do you think about...", "we might want to..."
- Acknowledge good decisions and clean patterns, not just problems
- When unsure, ask a clarifying question instead of assuming something is wrong

### What to Review

- **Correctness** — logic errors, edge cases, off-by-one errors
- **Readability** — naming consistency, code clarity, helpful error messages
- **Maintainability** — temporary workarounds tracked, types in the right crate, clean abstractions
- **Testability** — missing tests for new endpoints/logic, weakened assertions, coverage gaps
- **Performance** — unnecessary allocations in hot paths, unbounded response sizes, missing concurrency limits
- **Security** — auth checks on new routes, input validation, error message information leakage

### How to Structure Feedback

- Post a summary comment on the PR: overview of the changes, key observations, cross-cutting concerns
- Add inline comments at specific diff locations for targeted feedback
- Prefix minor style suggestions with `nit:` — these are optional and the author may skip them
- Do NOT prefix substantive feedback (correctness issues, missing tests) — these require attention

