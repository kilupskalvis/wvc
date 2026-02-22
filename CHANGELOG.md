# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-02-22

### Added
- Initial release
- Core commands: `init`, `status`, `add`, `reset`, `commit`, `log`, `diff`, `show`, `revert`
- Git-like staging area with `add`/`reset` workflow
- Vector tracking with separate property and vector change detection
- Exact vector restoration using binary storage
- Vector deduplication via content-addressable storage
- Schema change tracking
- Shell completions for bash, zsh, and fish
- Version command with build information
- **Branching**: `branch` command to create, list, and delete branches
- **Checkout**: `checkout` command to switch branches or checkout specific commits
  - Support for detached HEAD state
  - `-b` flag to create and switch in one step
  - `--force` flag to discard uncommitted changes
- **Merging**: `merge` command with full 3-way merge support
  - Fast-forward merge when possible
  - Merge commits with two parents for diverged branches
  - Conflict detection: modify-modify, delete-modify, add-add
  - `--ours` flag to auto-resolve conflicts with current branch version
  - `--theirs` flag to auto-resolve conflicts with incoming branch version
  - `--no-ff` flag to force merge commit even when fast-forward is possible
  - `-m` flag for custom merge commit messages
- Merge parent display in `log` command (shows `Merge: xxx yyy`)
- `[merge]` and `[schema]` tags in log output for easy identification
- **Stashing**: `stash` command to temporarily shelve uncommitted changes
  - `stash` / `stash push` to save staged and unstaged changes
  - `-m` flag for custom stash messages (default: `WIP on <branch>: <commit>`)
  - `stash list` to list all stashes (`stash@{0}` = newest)
  - `stash pop` to apply and remove a stash
  - `stash apply` to apply without removing
  - `--index` flag on `pop`/`apply` to re-stage previously staged changes
  - `stash drop` to remove a specific stash
  - `stash show` to display staged vs unstaged changes in a stash
  - `stash clear` to remove all stashes
  - `stash@{N}` reference syntax for targeting specific stashes
- **Reset modes**: `reset` command now supports `--soft`, `--mixed`, and `--hard` modes
  - `--soft`: Move HEAD and auto-stage changes from undone commits (like `git reset --soft`)
  - `--mixed`: Move HEAD and clear staging area (default)
  - `--hard`: Move HEAD, clear staging, and restore Weaviate to target state
  - Git-like positional argument syntax: `wvc reset --soft HEAD~1`
  - `-f` flag to skip confirmation for hard reset
  - Automatic disambiguation between commit references and class names
  - Use `--` to force class interpretation: `wvc reset -- main`
- **HEAD~N syntax**: Support for relative commit references (e.g., `HEAD~1`, `HEAD~3`)
- **Content-addressable commit IDs**: commit IDs are now SHA256 hashes derived from
  message, timestamp, parent ID, and a Merkle hash of all operations — guaranteeing
  deterministic, collision-free IDs across client and server
- **Remote management**: `remote` command to manage remote server connections
  - `remote add <name> <url>` to register a remote
  - `remote remove <name>` to deregister a remote
  - `remote list` to show all configured remotes
  - `remote set-token <name>` to store a bearer token (prompted, not echoed)
  - `remote info <name>` to show branch and commit counts from the server
  - Token resolution order: `WVC_REMOTE_TOKEN_<NAME>` env var → `WVC_REMOTE_TOKEN` env var → stored token
- **Push**: `push [remote] [branch]` to upload commits and vectors to a remote server
  - Negotiation phase to determine which commits and vectors are missing on the remote
  - Parallel vector upload with 4 concurrent workers
  - Commits uploaded in topological order (oldest first)
  - Compare-and-swap branch update — rejects push if remote has diverged (fetch first)
  - `--force` flag to bypass divergence check
  - `--delete <branch>` flag to delete a remote branch
- **Pull**: `pull [remote] [branch]` to fetch and integrate remote changes
  - Fast-forwards local branch when possible
  - Reports divergence and suggests `wvc merge` when branches have split
  - `--depth <n>` flag for shallow pull (last N commits only)
- **Fetch**: `fetch [remote] [branch]` to download remote changes without modifying the local branch
  - Updates remote-tracking branches (e.g., `origin/main`)
  - `--depth <n>` flag for shallow fetch
- **Shallow clones**: limit history depth on pull/fetch with `--depth N`
  - Reconstructs full Weaviate state from the shallow boundary forward
- **`wvc server` subcommand**: central remote server for team collaboration, built into the `wvc` binary (no separate installation)
  - `wvc server start` to run the server; admin token set via `WVC_ADMIN_TOKEN` env var
  - `wvc server repos create/list/delete` to manage repositories via the CLI
  - `wvc server tokens create/list/delete` to manage scoped access tokens via the CLI
  - Stores commit metadata in bbolt, vector blobs on the local filesystem
  - Bearer token authentication with per-repository permission scoping
  - Admin API for token provisioning (`/admin/tokens`) and garbage collection (`/admin/repos/{name}/gc`)
  - Rate limiting: configurable sliding window per token (default 300 req/min)
  - Per-repository write lock to prevent concurrent push and GC races
  - TLS support via `--tls-cert` / `--tls-key` flags
  - Webhook notifications on successful push with HMAC-SHA256 request signing
  - Structured JSON logging with request ID propagation
  - Gzip compression for commit bundle transfers
  - Graceful shutdown on SIGINT/SIGTERM
  - Default data directory: `~/.wvc-server` (override with `--data-dir` or `WVC_DATA_DIR`)
- **`wvc pull` Weaviate restore**: after a successful fast-forward, the local Weaviate instance is now updated to match the new HEAD (previously only the local commit history was updated)

### Changed
- Replaced SQLite with [bbolt](https://github.com/etcd-io/bbolt) for local storage —
  pure Go, no CGO required, consistent with the server-side metastore
- Operation identity now uses stable `(commit_id, seq)` pairs instead of
  auto-increment integers, enabling reliable transfer between client and server

### Fixed
- N/A
