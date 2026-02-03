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

### Stashing

```bash
wvc stash                                # Save uncommitted changes
wvc stash -m "work in progress"          # Save with a message
wvc stash list                           # List all stashes
wvc stash pop                            # Apply and remove latest stash
wvc stash pop --index                    # Apply and re-stage previously staged changes
wvc stash apply stash@{1}               # Apply a specific stash without removing
wvc stash show                           # Show changes in latest stash
wvc stash drop stash@{0}                # Remove a specific stash
wvc stash clear                          # Remove all stashes
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

### Remote Collaboration

```bash
wvc remote add origin https://wvc.example.com/myrepo   # Add a remote
wvc remote set-token origin                              # Set auth token (prompts for input)
wvc push                                                 # Push current branch to remote
wvc pull                                                 # Pull and fast-forward from remote
wvc fetch                                                # Download without modifying local branch
wvc push --force                                         # Force push (overwrites remote)
wvc push --delete origin feature                         # Delete a remote branch
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

### Stashing

| Command | Description |
|---------|-------------|
| `wvc stash` | Save all uncommitted changes |
| `wvc stash push [-m <message>]` | Save changes with an optional message |
| `wvc stash list` | List all stashes |
| `wvc stash pop [stash@{N}]` | Apply and remove a stash |
| `wvc stash pop --index [stash@{N}]` | Apply, re-stage previously staged changes, and remove |
| `wvc stash apply [stash@{N}]` | Apply a stash without removing it |
| `wvc stash apply --index [stash@{N}]` | Apply and re-stage without removing |
| `wvc stash drop [stash@{N}]` | Remove a specific stash |
| `wvc stash show [stash@{N}]` | Show changes in a stash |
| `wvc stash clear` | Remove all stashes |

### Remote Collaboration

| Command | Description |
|---------|-------------|
| `wvc remote` | List all configured remotes |
| `wvc remote -v` | List remotes with URLs |
| `wvc remote add <name> <url>` | Add a remote repository |
| `wvc remote remove <name>` | Remove a remote |
| `wvc remote set-url <name> <url>` | Change a remote's URL |
| `wvc remote set-token <name>` | Set authentication token (reads from stdin) |
| `wvc remote info <name>` | Show remote repository stats |
| `wvc push [<remote>] [<branch>]` | Push commits and vectors to a remote |
| `wvc push --force` | Force push (overwrites remote branch) |
| `wvc push --delete <remote> <branch>` | Delete a branch on the remote |
| `wvc pull [<remote>] [<branch>]` | Fetch and fast-forward the local branch |
| `wvc pull --depth <n>` | Pull only the last n commits |
| `wvc fetch [<remote>] [<branch>]` | Download commits without modifying local branch |
| `wvc fetch --depth <n>` | Fetch only the last n commits |

## Team Collaboration Example

This walkthrough shows two team members (Alice and Bob) collaborating on a shared Weaviate database through a `wvc-server`.

### 1. Start the server

```bash
wvc-server \
  -data-dir /var/lib/wvc-server \
  -listen 0.0.0.0:8720 \
  -admin-token "$ADMIN_TOKEN"
```

Create a repository directory and an API token:

```bash
mkdir -p /var/lib/wvc-server/repos/myproject

curl -X POST https://wvc.example.com/admin/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"description": "team token", "repos": ["myproject"], "permission": "rw"}'
# returns: {"token": "wvc_abc123..."}
```

### 2. Alice: initialize and push

Alice connects to the team Weaviate instance, commits the current state, and pushes it to the server:

```bash
wvc init --url http://weaviate:8080
wvc commit -m "Initial schema and seed data"
wvc remote add origin https://wvc.example.com/myproject
wvc remote set-token origin            # paste the team token when prompted
wvc push
```

### 3. Bob: clone via pull

Bob initializes against the same Weaviate instance and pulls Alice's history:

```bash
wvc init --url http://weaviate:8080
wvc remote add origin https://wvc.example.com/myproject
wvc remote set-token origin
wvc pull
```

Bob now has all of Alice's commits locally and can browse them with `wvc log`.

### 4. Bob: work on a feature branch

```bash
wvc checkout -b add-products
# ... insert Product objects into Weaviate ...
wvc add Product
wvc commit -m "Add product catalog"
wvc push origin add-products
```

### 5. Alice: review and merge

Alice fetches Bob's branch, inspects it, and merges it into main:

```bash
wvc fetch origin add-products
wvc log origin/add-products            # review Bob's commits
wvc merge origin/add-products          # merge into current branch
wvc push
```

### 6. Handling divergence

If Alice and Bob both push to `main`, the second push is rejected:

```
$ wvc push
error: push rejected: remote has diverged; pull first or use --force
```

Pull to detect the divergence, merge, and push again:

```bash
wvc pull                               # fetches and reports divergence
wvc merge origin/main                  # integrate remote changes
wvc push                               # push the merged result
```

## Features

- **Staging area**: Git-like `add`/`reset` workflow for selective commits
- **Reset modes**: Soft, mixed, and hard reset with `HEAD~N` syntax support
- **Vector tracking**: Detects property-only, vector-only, or combined changes
- **Exact restoration**: Vectors restored bit-for-bit on revert
- **Deduplication**: Identical vectors stored once via content-addressable storage
- **Branching**: Create, switch, and delete branches for parallel development
- **Merging**: Fast-forward and 3-way merge with conflict detection
- **Conflict resolution**: Auto-resolve conflicts with `--ours` or `--theirs` flags
- **Stashing**: Shelve uncommitted changes and restore them later with `--index` support
- **Schema tracking**: Track schema changes (new classes, properties) alongside data
- **Remote collaboration**: Push/pull/fetch with a central `wvc-server` for team workflows
- **Token authentication**: Scoped read-only or read-write tokens per repository
- **Shallow fetch**: Download only recent history with `--depth`
- **Force push**: Overwrite remote history when needed

## How It Works

1. `wvc init` snapshots the current Weaviate state
2. `wvc status` compares current state against last known state
3. Changes are recorded as operations (insert, update, delete)
4. `wvc revert` replays operations in reverse
5. `wvc branch` creates named references to commits
6. `wvc checkout` restores the Weaviate state to match a branch or commit
7. `wvc merge` combines changes from different branches using 3-way merge
8. `wvc stash` saves uncommitted changes and restores Weaviate to a clean state
9. `wvc push` uploads commits and vectors to a remote `wvc-server`
10. `wvc pull` downloads remote commits and fast-forwards the local branch
11. `wvc fetch` downloads remote commits without modifying the local branch

Data is stored locally in `.wvc/`:
- `config` - Weaviate URL and server version
- `wvc.db` - Commits, branches, operations, and vector blobs

## wvc-server

The remote server stores repositories and handles push/pull negotiation. Each repository is isolated with its own metadata database and blob storage.

```bash
wvc-server \
  -data-dir /var/lib/wvc-server \
  -listen 0.0.0.0:8720 \
  -admin-token "$ADMIN_TOKEN"
```

| Flag | Default | Description |
|------|---------|-------------|
| `-data-dir` | `/var/lib/wvc-server` | Root directory for repository data |
| `-listen` | `0.0.0.0:8720` | Address and port to listen on |
| `-admin-token` | | Token for admin API (create tokens, run GC) |
| `-tls-cert` | | TLS certificate file |
| `-tls-key` | | TLS private key file |
| `-webhook-urls` | | Comma-separated URLs to notify on push |
| `-log-level` | `info` | Log level: debug, info, warn, error |
| `-log-format` | `json` | Log format: json, text |

### Admin API

Create a scoped token:

```bash
curl -X POST https://wvc.example.com/admin/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"description": "ci-readonly", "repos": ["myproject"], "permission": "ro"}'
```

Run garbage collection on a repository:

```bash
curl -X POST https://wvc.example.com/admin/repos/myproject/gc \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

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
