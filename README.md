<table align="center"><tr><td>
<pre>
██╗    ██╗██╗   ██╗ ██████╗
██║    ██║██║   ██║██╔════╝
██║ █╗ ██║██║   ██║██║
██║███╗██║╚██╗ ██╔╝██║
╚███╔███╔╝ ╚████╔╝ ╚██████╗
 ╚══╝╚══╝   ╚═══╝   ╚═════╝
</pre>
</td></tr></table>

<p align="center">
<b>Weaviate Version Control</b><br>
A git-like CLI tool for version controlling Weaviate databases
</p>

<p align="center">
<a href="https://go.dev/"><img src="https://img.shields.io/github/go-mod/go-version/kilupskalvis/wvc" alt="Go Version"></a>
<a href="https://github.com/kilupskalvis/wvc/releases"><img src="https://img.shields.io/github/v/release/kilupskalvis/wvc" alt="Release"></a>
<a href="LICENSE"><img src="https://img.shields.io/github/license/kilupskalvis/wvc" alt="License"></a>
</p>

## Installation

### Homebrew (macOS/Linux)

```bash
brew install kilupskalvis/tap/wvc
```

### Install Script

```bash
curl -sSL https://raw.githubusercontent.com/kilupskalvis/wvc/main/install.sh | sh
```

### Go Install

```bash
go install github.com/kilupskalvis/wvc@latest
```

### Manual Download

Download the latest binary from [GitHub Releases](https://github.com/kilupskalvis/wvc/releases).

### Build from Source

```bash
git clone https://github.com/kilupskalvis/wvc.git
cd wvc
make build
```

## Quick Start

```bash
wvc init --url http://localhost:8080    # Initialize tracking
wvc status                               # Check for changes
wvc add .                                # Stage all changes
wvc commit -m "Initial data load"        # Commit
wvc log                                  # View history
wvc revert HEAD                          # Undo last commit
wvc reset --soft --to HEAD~1             # Undo commit, keep changes staged
wvc reset --hard --to main               # Discard all and reset to main
```

### Branching & Merging

```bash
wvc branch feature                       # Create a new branch
wvc checkout feature                     # Switch to branch
wvc checkout -b hotfix                   # Create and switch in one step
wvc branch                               # List all branches
wvc merge feature                        # Merge branch into current
wvc merge --theirs feature               # Merge, prefer incoming on conflict
```

## Commands

### Basic Commands

| Command | Description |
|---------|-------------|
| `wvc init --url <url>` | Initialize a new WVC repository |
| `wvc status` | Show uncommitted changes |
| `wvc add [<class> \| <class>/<id> \| .]` | Stage changes for commit |
| `wvc reset [<class>/<id>]` | Unstage changes |
| `wvc reset --soft <commit>` | Soft reset: move HEAD, auto-stage undone changes |
| `wvc reset <commit>` | Mixed reset: move HEAD, clear staging (default) |
| `wvc reset --hard <commit>` | Hard reset: move HEAD, restore Weaviate state |
| `wvc commit -m "<message>" [-a]` | Commit staged changes |
| `wvc diff [--stat]` | Show detailed changes |
| `wvc log [--oneline] [-n <count>]` | Show commit history |
| `wvc show [<commit>]` | Show commit details |
| `wvc revert <commit>` | Revert a commit |

### Branching & Merging

| Command | Description |
|---------|-------------|
| `wvc branch` | List all branches |
| `wvc branch <name>` | Create a new branch |
| `wvc branch -d <name>` | Delete a branch |
| `wvc checkout <branch>` | Switch to a branch |
| `wvc checkout <commit>` | Checkout a specific commit (detached HEAD) |
| `wvc checkout -b <name>` | Create and switch to a new branch |
| `wvc merge <branch>` | Merge branch into current branch |
| `wvc merge --no-ff <branch>` | Merge with a merge commit (no fast-forward) |
| `wvc merge --ours <branch>` | Merge, prefer current branch on conflicts |
| `wvc merge --theirs <branch>` | Merge, prefer incoming branch on conflicts |
| `wvc merge -m "<msg>" <branch>` | Merge with a custom commit message |

## Features

- **Staging area**: Git-like `add`/`reset` workflow for selective commits
- **Reset modes**: Soft, mixed, and hard reset with `HEAD~N` syntax support
- **Vector tracking**: Detects property-only, vector-only, or combined changes
- **Exact restoration**: Vectors restored bit-for-bit on revert
- **Deduplication**: Identical vectors stored once via content-addressable storage
- **Branching**: Create, switch, and delete branches for parallel development
- **Merging**: Fast-forward and 3-way merge with conflict detection
- **Conflict resolution**: Auto-resolve conflicts with `--ours` or `--theirs` flags
- **Schema tracking**: Track schema changes (new classes, properties) alongside data

## How It Works

1. `wvc init` snapshots the current Weaviate state
2. `wvc status` compares current state against last known state
3. Changes are recorded as operations (insert, update, delete)
4. `wvc revert` replays operations in reverse
5. `wvc branch` creates named references to commits
6. `wvc checkout` restores the Weaviate state to match a branch or commit
7. `wvc merge` combines changes from different branches using 3-way merge

Data is stored locally in `.wvc/`:
- `config` - Weaviate URL and server version
- `wvc.db` - SQLite database with commits, branches, and vector blobs

## Requirements

- Go 1.21+
- Weaviate 1.14+

## Development

```bash
make setup    # Install git hooks
make test     # Run tests
make check    # Run all pre-commit checks
```

## License

MIT
