# Agent Guide

This file gives automated agents a small, repo-specific playbook for commits and releases in JustLogV2.

## Commit Guide

### Commit Message Format

Use conventional-style prefixes:

- `init:` for first-time repo/bootstrap work
- `feat:` for user-visible functionality
- `fix:` for bug fixes
- `refactor:` for code structure changes without behavior changes
- `chore:` for maintenance, tooling, dependency, or release bookkeeping
- `docs:` for documentation-only changes
- `test:` for test code changes that don't affect runtime behavior

Format:

```text
<type>: <short imperative summary>
```

Examples:

- `feat: add admin channel removal endpoint`
- `fix: deduplicate mirrored IRC events across lanes`
- `refactor: split log request parsing from response rendering`
- `chore: bump release version`
- `docs: add codebase guide`

### Commit Writing Rules

- Keep the subject line short and specific.
- Use imperative mood: `add`, `fix`, `bump`, `document`.
- Do not mix unrelated changes in one commit.
- Prefer one logical change per commit.
- If a change affects runtime behavior, tests should be updated or run when practical.

### What To Commit

Agents should normally commit:

- source changes under `src/`
- test updates under `tests/`
- build/config changes such as `Cargo.toml`, `Cargo.lock`, `Dockerfile`, or workflow updates
- documentation when it materially helps future work

### Documentation Expectations

- When adding a new meaningful feature, update the relevant documentation in the same change when practical.
- Treat user-visible behavior, new endpoints, config changes, and operational workflow changes as documentation-sensitive by default.
- If a feature does not need a docs update, agents should make that a conscious decision rather than silently skipping docs.

Agents should normally avoid committing:

- `target/`
- local configs such as `config.json`
- SQLite databases, logs, or other generated runtime data
- machine-local deployment hacks unless explicitly requested

## Release Guide

### Current Release Mechanism

Releases are tag-driven.

This repo publishes Docker images from [docker-publish.yml](C:/Users/Albert/Sync/Projects/JustLogV2/.github/workflows/docker-publish.yml):

- pushes to `main` publish `latest` and a short SHA image
- pushes of tags matching `v*.*.*` publish a versioned image
- images are published to `ghcr.io/alb11747/justlogv2`

### Versioning Rules

Use semantic versioning in [Cargo.toml](C:/Users/Albert/Sync/Projects/JustLogV2/Cargo.toml):

- patch: bug fixes, small safe improvements
- minor: new backward-compatible features
- major: breaking changes

Version tags must be prefixed with `v`, for example:

- `v0.1.3`
- `v0.2.0`
- `v1.0.0`

### Release Procedure

When asked to cut a release, agents should:

1. Confirm the working tree is clean or only contains intended release changes.
2. Update the version in [Cargo.toml](C:/Users/Albert/Sync/Projects/JustLogV2/Cargo.toml).
3. Run `cargo check` or stronger verification if requested.
4. Commit the version bump with:

```text
chore: bump release version
```

5. Create an annotated tag named `vX.Y.Z` on that commit.
6. Push the commit and tag.

Example:

```powershell
git add Cargo.toml Cargo.lock
git commit -m "chore: bump release version"
git tag -a v0.1.3 -m "v0.1.3"
git push origin main
git push origin v0.1.3
```

### Release Safety Notes

- Do not create or move release tags casually.
- If tags or commit history have already been published, rewriting history requires explicit user approval.
- Keep release commits focused on versioning and any directly related metadata.
- If the release depends on documentation or deployment notes, commit those separately unless the user asks otherwise.

## Practical Defaults For Agents

- For normal feature work, prefer `feat:`, `fix:`, `refactor:`, `docs:`, or `chore:` as appropriate.
- For initial repository bootstrapping or first publication setup, use `init:`.
- For a version bump intended to produce a release tag, use `chore: bump release version`.
- If uncertain whether a change is user-visible, choose the prefix based on the primary intent of the commit rather than trying to encode every side effect.
