# Agent Guidance

## Project Context

This repository contains the `github.com/buildkite/buildkite-logs` Go library and
the `bklog` CLI for parsing, caching, searching, and querying Buildkite logs.

The project is still in active `0.x` development. Public APIs are useful and may
already have consumers, but they should not be treated as stable 1.0 contracts.
It is acceptable to make public API changes when they improve the library, fix a
recent API mistake, or better match the intended design. Prefer making those
changes cleanly instead of adding compatibility shims for unreleased or
experimental branch work.

When changing public APIs, update examples, docs, and tests in the same change.
If a release is involved, use a `0.x` minor version bump for breaking API changes
and reserve patch bumps for compatible bug fixes.

## Development Practices

- Follow the existing Go style and keep changes scoped to the requested behavior.
- Prefer existing helpers and package boundaries over adding new abstractions.
- Preserve behavior for persisted data, shipped releases, and documented user
  workflows unless the task explicitly calls for changing them.
- Keep the CLI useful for development and debugging, but do not couple library
  behavior to CLI-only assumptions.
- Add or update focused tests for parser, cache, query, or client behavior when
  changing those areas.

## Useful Commands

- `make fmt` formats Go code.
- `make test` runs the full Go test suite.
- `make lint` runs `golangci-lint`.
- `make ci` runs dependency tidy, formatting checks, tests, linting, and build.
- `make dev` builds the development `bklog` binary quickly.

