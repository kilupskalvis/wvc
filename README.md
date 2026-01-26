<div align="center">
<pre style="display: inline-block; text-align: left;">
██╗    ██╗██╗   ██╗ ██████╗
██║    ██║██║   ██║██╔════╝
██║ █╗ ██║██║   ██║██║
██║███╗██║╚██╗ ██╔╝██║
╚███╔███╔╝ ╚████╔╝ ╚██████╗
 ╚══╝╚══╝   ╚═══╝   ╚═════╝
</pre>

<strong>Weaviate Version Control</strong>
<br>
<em>A git-like CLI tool for version controlling Weaviate databases</em>
<br><br>

[![CI](https://github.com/kilupskalvis/wvc/actions/workflows/ci.yml/badge.svg)](https://github.com/kilupskalvis/wvc/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/kilupskalvis/wvc)](https://go.dev/)
[![Release](https://img.shields.io/github/v/release/kilupskalvis/wvc)](https://github.com/kilupskalvis/wvc/releases)
[![License](https://img.shields.io/github/license/kilupskalvis/wvc)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/kilupskalvis/wvc)](https://goreportcard.com/report/github.com/kilupskalvis/wvc)

</div>

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
```

## Commands

| Command | Description |
|---------|-------------|
| `wvc init --url <url>` | Initialize a new WVC repository |
| `wvc status` | Show uncommitted changes |
| `wvc add [<class> \| <class>/<id> \| .]` | Stage changes for commit |
| `wvc reset [<class>/<id>]` | Unstage changes |
| `wvc commit -m "<message>" [-a]` | Commit staged changes |
| `wvc diff [--stat]` | Show detailed changes |
| `wvc log [--oneline] [-n <count>]` | Show commit history |
| `wvc show [<commit>]` | Show commit details |
| `wvc revert <commit>` | Revert a commit |

## Features

- **Staging area**: Git-like `add`/`reset` workflow for selective commits
- **Vector tracking**: Detects property-only, vector-only, or combined changes
- **Exact restoration**: Vectors restored bit-for-bit on revert
- **Deduplication**: Identical vectors stored once via content-addressable storage

## How It Works

1. `wvc init` snapshots the current Weaviate state
2. `wvc status` compares current state against last known state
3. Changes are recorded as operations (insert, update, delete)
4. `wvc revert` replays operations in reverse

Data is stored locally in `.wvc/`:
- `config` - Weaviate URL and server version
- `wvc.db` - SQLite database with commits and vector blobs

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
