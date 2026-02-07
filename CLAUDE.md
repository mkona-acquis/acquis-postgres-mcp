# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About This Fork

This is a fork of [crystaldba/postgres-mcp](https://github.com/crystaldba/postgres-mcp) with additional functionality and customizations. The original project provides excellent PostgreSQL database tuning and analysis capabilities through the Model Context Protocol (MCP).

**Repository**: `mkona-acquis/acquis-postgres-mcp`

**Goals for this fork:**
- Add custom functionality for our team's specific use cases
- Maintain compatibility with upstream features
- Distribute via uvx for easy colleague access
- [Add specific new features here as they are developed]

**Syncing with upstream:**
```bash
# Add upstream remote (if not already added)
git remote add upstream https://github.com/crystaldba/postgres-mcp.git

# Fetch upstream changes
git fetch upstream

# Merge upstream changes into main
git merge upstream/main
```

## Project Overview

Postgres MCP Pro is a Model Context Protocol (MCP) server that provides PostgreSQL database tuning, analysis, and safe SQL execution capabilities. It enables AI assistants to interact with PostgreSQL databases through a standardized MCP interface, offering features like index tuning, query plan analysis, database health checks, and schema intelligence.

## Development Commands

### Setup
```bash
# Install dependencies
uv sync

# Build the package
uv build
```

### Running the Server
```bash
# Run with stdio transport (default)
uv run acquis-postgres-mcp "postgresql://user:password@host:port/dbname" --access-mode=unrestricted

# Run with SSE transport
uv run acquis-postgres-mcp "postgresql://user:password@host:port/dbname" --access-mode=unrestricted --transport=sse --sse-host=localhost --sse-port=8000

# Run with streamable HTTP transport
uv run acquis-postgres-mcp "postgresql://user:password@host:port/dbname" --access-mode=unrestricted --transport=streamable-http --streamable-http-host=localhost --streamable-http-port=8000

# Run with restricted (read-only) mode
uv run acquis-postgres-mcp "postgresql://user:password@host:port/dbname" --access-mode=restricted
```

### Testing
```bash
# Run all tests
uv run pytest

# Run all tests with verbose output and logging
uv run pytest -v --log-cli-level=INFO

# Run a specific test file
uv run pytest tests/unit/sql/test_obfuscate_password.py

# Run a specific test
uv run pytest tests/unit/sql/test_db_conn_pool.py::test_pool_connect_success
```

### Linting and Type Checking
```bash
# Format code
uv run ruff format .

# Check code formatting
uv run ruff format --check .

# Run linting
uv run ruff check .

# Run type checking
uv run pyright
```

### Installation for Colleagues

**Using uvx (Recommended for team members):**
```bash
# Once published, colleagues can run directly with uvx:
uvx acquis-postgres-mcp "postgresql://user:password@host:port/dbname" --access-mode=unrestricted

# Or from this repository/branch (before publishing):
uvx --from git+https://github.com/mkona-acquis/acquis-postgres-mcp.git acquis-postgres-mcp "postgresql://..." --access-mode=unrestricted
```

**Installing from source:**
```bash
# Clone the repository
git clone https://github.com/mkona-acquis/acquis-postgres-mcp.git
cd acquis-postgres-mcp

# Install with pipx
pipx install .

# Or install with uv
uv pip install .

# Then run with:
acquis-postgres-mcp "postgresql://..." --access-mode=unrestricted
```

### Publishing to PyPI (for uvx access)

To make this available via uvx for your colleagues:

```bash
# 1. Update version in pyproject.toml
# 2. Build the package
uv build

# 3. Publish to PyPI (requires PyPI credentials)
uv publish

# Or publish to Test PyPI first
uv publish --publish-url https://test.pypi.org/legacy/
```

After publishing, colleagues can simply run:
```bash
uvx acquis-postgres-mcp "postgresql://user:password@host:port/dbname" --access-mode=unrestricted
```

## Architecture

### Core Components

**MCP Server (`server.py`)**
- Entry point that defines all MCP tools using FastMCP framework
- Manages database connection pooling via `DbConnPool`
- Implements two access modes:
  - `UNRESTRICTED`: Full read/write access (for development)
  - `RESTRICTED`: Read-only with safety features and execution timeouts (for production)
- Supports three transports: stdio, SSE, and streamable-http

**SQL Driver Layer (`sql/`)**
- `DbConnPool`: Manages psycopg3 async connection pool (min_size=1, max_size=5)
- `SqlDriver`: Base adapter that wraps PostgreSQL connections
- `SafeSqlDriver`: Extends SqlDriver with read-only enforcement, SQL parsing via pglast, and query timeouts
- `obfuscate_password()`: Security utility that redacts passwords from connection strings and error messages

**Index Tuning (`index/`)**
- `DatabaseTuningAdvisor (dta_calc.py)`: Implements Microsoft SQL Server's "Anytime Algorithm" for index optimization
  - Uses greedy search strategy with Pareto front cost-benefit analysis
  - Generates candidate indexes by parsing SQL queries and identifying columns used in filters, joins, grouping, and sorting
  - Leverages HypoPG extension to simulate index performance without actually creating them
  - Configurable parameters: budget_mb, max_runtime_seconds, max_index_width, pareto_alpha
- `LLMOptimizerTool (llm_opt.py)`: Experimental approach using LLM-based optimization with iterative refinement
- `IndexTuningBase (index_opt_base.py)`: Base class providing common functionality for both approaches
- `TextPresentation (presentation.py)`: Formats index recommendations and analysis results for display

**Database Health (`database_health/`)**
- Modular health check system adapted from PgHero
- Each calculator focuses on specific aspect:
  - `IndexHealthCalc`: Invalid, duplicate, bloated, and unused indexes
  - `BufferHealthCalc`: Buffer cache hit rates for indexes and tables
  - `ConnectionHealthCalc`: Connection count and utilization
  - `VacuumHealthCalc`: Transaction ID wraparound danger
  - `SequenceHealthCalc`: Sequences approaching maximum values
  - `ReplicationCalc`: Replication lag and slot usage
  - `ConstraintHealthCalc`: Invalid constraints

**Query Analysis**
- `ExplainPlanTool (explain/explain_plan.py)`: Generates and formats EXPLAIN/EXPLAIN ANALYZE plans, supports hypothetical indexes via HypoPG
- `TopQueriesCalc (top_queries/top_queries_calc.py)`: Analyzes pg_stat_statements data to identify slow queries and resource-intensive workloads

**Safety Features (`sql/safe_sql.py`)**
- Enforces read-only transactions when in RESTRICTED mode
- Parses SQL using pglast to reject COMMIT/ROLLBACK statements that could bypass read-only mode
- Implements query timeouts to prevent long-running queries
- Note: Unsafe stored procedure languages (if enabled) can circumvent protections

### Key Design Decisions

**Connection Management**: Uses psycopg3 (with libpq) over asyncpg for full Postgres feature support and community-backed implementation. Psycopg3's async support has matured to be competitive with asyncpg performance.

**Parameter Binding**: When normalizing queries from pg_stat_statements, parameter values are lost. The system generates realistic parameter values by sampling from table statistics to produce accurate EXPLAIN plans.

**Search Strategy**: DTA uses greedy search (find best single index, then best addition, etc.) with time budget and improvement threshold cutoffs. This balances solution quality against runtime, following the "anytime algorithm" approach.

**MCP API Design**: Exposes all functionality via MCP tools (not resources) for maximum client compatibility. This differs from the Reference PostgreSQL MCP Server which uses resources for schema information.

## Testing Strategy

- **Unit Tests** (`tests/unit/`): Test individual components in isolation
- **Integration Tests** (`tests/integration/`): Test components against real PostgreSQL databases
- Integration tests use Docker containers with PostgreSQL + HypoPG extension
- Test configuration in `tests/Dockerfile.postgres-hypopg`
- Pytest configuration: `pythonpath = ["./src"]`, `asyncio_default_fixture_loop_scope = "function"`

## PostgreSQL Extension Requirements

**Required for Full Functionality:**
- `pg_stat_statements`: Required for workload analysis and top queries functionality
- `hypopg`: Required for index tuning (simulates hypothetical indexes without creating them)

Both extensions are typically pre-installed on managed services (AWS RDS, Azure SQL, Google Cloud SQL) and just need `CREATE EXTENSION` commands.

## CI/CD

- GitHub Actions workflow: `.github/workflows/build.yml`
- Runs on push to main and pull requests
- Steps: build (uv sync), lint (ruff format + ruff check), type check (pyright), test (pytest)
- Released to PyPI as `acquis-postgres-mcp` package

## Code Style

- Line length: 150 characters
- Formatter: ruff (configured in pyproject.toml)
- Type checking: pyright with standard mode
- Import organization: force-single-line, known-first-party=["acquis-postgres-mcp"]
- Python version target: 3.12+

## Adding New Features

When adding new MCP tools to this fork:

1. **Add the tool in `server.py`**:
   - Use the `@mcp.tool()` decorator
   - Add appropriate `description` and `annotations` (ToolAnnotations with readOnlyHint or destructiveHint)
   - Follow the pattern of existing tools for error handling and response formatting

2. **Create supporting modules**:
   - Add new functionality in appropriate subdirectories (e.g., `sql/`, `database_health/`, `index/`)
   - Follow the existing pattern of having a calculator/tool class that takes `sql_driver` as a parameter
   - Keep business logic separate from the MCP tool definition

3. **Add tests**:
   - Unit tests in `tests/unit/` for isolated component testing
   - Integration tests in `tests/integration/` if database interaction is needed
   - Use pytest fixtures from `tests/conftest.py`

4. **Update CLAUDE.md**:
   - Document new tools in the "About This Fork" section
   - Add any new architecture components to the "Core Components" section
   - Update commands if new dependencies or setup steps are required

5. **Version and release**:
   - Update version in `pyproject.toml`
   - Run `uv sync` to update `uv.lock`
   - Build and publish: `uv build && uv publish` (or to test PyPI first)
