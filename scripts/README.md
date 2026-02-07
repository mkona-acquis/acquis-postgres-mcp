# Publishing Scripts

This directory contains scripts to automate the publishing process for `acquis-postgres-mcp`.

## 📜 Scripts

### `publish.sh` - Full Interactive Publish (Recommended)

Complete interactive publishing workflow with all quality checks.

**Usage:**
```bash
./scripts/publish.sh
```

**Features:**
- ✅ Automatic patch version bump (e.g., 0.3.1 → 0.3.2)
- ✅ Runs all tests
- ✅ Linting and formatting
- ✅ Type checking
- ✅ Cleans old builds
- ✅ Builds fresh package
- ✅ Publishes to PyPI or Test PyPI
- ✅ Optional git commit and push
- ✅ Colorful output with progress indicators

**Example Session:**
```
📦 Current version: 0.3.1
📦 New version will be: 0.3.2

Continue with version 0.3.2? [y/N]: y

[Runs all checks and publishes...]
```

### `quick-publish.sh` - Quick Patch Version Bump

Fast publish for when you've already tested and just need to publish.

**Usage:**
```bash
./scripts/quick-publish.sh
```

**What it does:**
1. Bumps PATCH version (e.g., 0.3.1 → 0.3.2)
2. Updates lock file
3. Cleans and builds
4. Publishes to PyPI

**⚠️ Warning:** Skips tests and validation. Use only when confident!

## 🔧 Manual Publishing

If you prefer manual control:

```bash
# 1. Update version in pyproject.toml manually
vim pyproject.toml  # Change version = "0.3.1" to "0.4.0"

# 2. Sync and test
uv sync
uv run pytest tests/unit/ -v
uv run ruff check .
uv run pyright src/postgres_mcp/

# 3. Build and publish
rm -rf dist/
uv build
uv publish
```

## 📦 Version Numbering Guide

Follow [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`

**Scripts automatically bump PATCH version** (0.3.1 → 0.3.2):
- Bug fixes
- Documentation updates
- Small improvements
- **This is what the scripts do by default**

**For MINOR version bumps** (0.3.2 → 0.4.0):
- New features
- New tools added
- **Manually edit version in `pyproject.toml` first**, then run script

**For MAJOR version bumps** (0.3.2 → 1.0.0):
- Breaking changes
- Incompatible API changes
- **Manually edit version in `pyproject.toml` first**, then run script

## 🧪 Testing Before Production

To test on Test PyPI first:

```bash
# Using publish.sh - select Test PyPI when prompted
./scripts/publish.sh

# Or manually
uv publish --publish-url https://test.pypi.org/legacy/

# Test installation
uvx --from https://test.pypi.org/simple/ acquis-postgres-mcp "postgresql://..."
```

## 🚨 Common Issues

### "File already exists" Error

**Problem:** Old build artifacts in `dist/` folder.

**Solution:**
```bash
rm -rf dist/
uv build
uv publish
```

### "Version already published"

**Problem:** PyPI doesn't allow re-uploading the same version.

**Solution:** Increment the version number in `pyproject.toml`.

### Permission Denied

**Problem:** Scripts not executable.

**Solution:**
```bash
chmod +x scripts/*.sh
```

### PyPI Authentication

**Problem:** No PyPI credentials configured.

**Solution:** Configure PyPI token:
```bash
# Create ~/.pypirc
cat > ~/.pypirc << EOF
[pypi]
username = __token__
password = pypi-YOUR_TOKEN_HERE
EOF

chmod 600 ~/.pypirc
```

Or use environment variable:
```bash
export UV_PUBLISH_TOKEN=pypi-YOUR_TOKEN_HERE
```

## 📚 After Publishing

Once published to PyPI, users can install with:

```bash
# Run directly (recommended)
uvx acquis-postgres-mcp "postgresql://user:pass@host:port/db" --access-mode=unrestricted

# Or install globally
pipx install acquis-postgres-mcp
```

## 🔍 Verify Published Package

Check the package on PyPI:
- Production: https://pypi.org/project/acquis-postgres-mcp/
- Test: https://test.pypi.org/project/acquis-postgres-mcp/

Test installation immediately after publishing:
```bash
# Wait ~1 minute for PyPI to propagate, then:
uvx acquis-postgres-mcp --help
```
