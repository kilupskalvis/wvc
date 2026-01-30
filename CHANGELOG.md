# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  - Database migration to v2 for existing repositories
- **Reset modes**: `reset` command now supports `--soft`, `--mixed`, and `--hard` modes
  - `--soft`: Move HEAD and auto-stage changes from undone commits (like `git reset --soft`)
  - `--mixed`: Move HEAD and clear staging area (default)
  - `--hard`: Move HEAD, clear staging, and restore Weaviate to target state
  - Git-like positional argument syntax: `wvc reset --soft HEAD~1`
  - `-f` flag to skip confirmation for hard reset
  - Automatic disambiguation between commit references and class names
  - Use `--` to force class interpretation: `wvc reset -- main`
- **HEAD~N syntax**: Support for relative commit references (e.g., `HEAD~1`, `HEAD~3`)

### Changed
- N/A

### Fixed
- N/A
