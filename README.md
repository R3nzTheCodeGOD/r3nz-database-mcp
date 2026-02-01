# r3nz-database-mcp

Modern Rust MCP server for AI-powered database analysis. Connect Claude to MSSQL and PostgreSQL databases using natural language.

## Platform Support

| Platform | Status |
|----------|--------|
| Windows x64 | ✅ Pre-built binary |
| macOS / Linux | 🔧 Build from source |

## Installation

### Windows - Plugin (Recommended)

```bash
# In Claude Code
/plugin marketplace add github:R3nzTheCodeGOD/r3nz-database-mcp
/plugin install r3nz-database-mcp@r3nz-database-tools --scope user
```

### Windows - Direct MCP

```bash
# Download binary from GitHub Releases, then:
claude mcp add --transport stdio r3nz-database -- C:\path\to\r3nz-database-mcp-windows-x64.exe
```

### macOS / Linux - Build from Source

```bash
# Prerequisites (Linux only)
# Ubuntu/Debian: sudo apt install libssl-dev pkg-config
# Fedora: sudo dnf install openssl-devel

git clone https://github.com/R3nzTheCodeGOD/r3nz-database-mcp.git
cd r3nz-database-mcp
cargo build --release

claude mcp add --transport stdio r3nz-database -- ./target/release/r3nz-database-mcp
```

## Quick Start

No configuration needed. Just tell Claude what you want:

```
"Connect to my database: Server=192.168.1.100,1433;Database=MyDb;User Id=admin;Password=secret"

"Show me all tables"

"What are the top 10 orders by total amount?"
```

## How It Works

1. **Start without connection** - Server launches ready to receive commands
2. **Connect via natural language** - Tell Claude your connection details
3. **Query and analyze** - Use all database tools through conversation

### Connection Formats

**ADO.NET:**
```
Server=localhost,1433;Database=MyDb;User Id=sa;Password=secret
```

**URL:**
```
mssql://user:pass@localhost:1433/database
postgres://user:pass@localhost:5432/database
```

## Available Tools

### Connection
| Tool | Description |
|------|-------------|
| `connect` | Connect to database |
| `disconnect` | Close connection |
| `connection_info` | Check status |
| `list_databases` | List databases on server |
| `switch_database` | Switch database |

### Schema
| Tool | Description |
|------|-------------|
| `list_tables` | List tables/views |
| `get_schema` | Column details |
| `get_relationships` | Foreign keys |

### Query
| Tool | Description |
|------|-------------|
| `execute_query` | Run SELECT queries |

### Performance
| Tool | Description |
|------|-------------|
| `analyze_query` | Execution plan |
| `get_performance_stats` | Metrics |
| `get_index_usage` | Index stats |
| `find_blocking_queries` | Blocking queries |

## Security

- **Read-only**: Only SELECT queries allowed
- **SQL Validation**: Injection protection
- **Rate Limiting**: Request throttling
- **No Storage**: Credentials never stored

## License

MIT
