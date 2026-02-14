#!/bin/bash
set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  Acquis Postgres MCP - Publish Script${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo -e "${RED}❌ Error: pyproject.toml not found. Run this script from the project root.${NC}"
    exit 1
fi

# Get current version from pyproject.toml
CURRENT_VERSION=$(grep -E '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')
echo -e "${BLUE}📦 Current version: ${YELLOW}${CURRENT_VERSION}${NC}"

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Always bump patch version
NEW_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))"

echo
echo -e "${GREEN}📦 New version will be: ${YELLOW}${NEW_VERSION}${NC}"
echo

# Confirm before proceeding
read -p "Continue with version ${NEW_VERSION}? [y/N]: " CONFIRM
if [[ ! $CONFIRM =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}⚠️  Publish cancelled${NC}"
    exit 0
fi

# Rollback function
rollback_version() {
    echo
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}⚠️  Error detected - Rolling back version${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/^version = \".*\"/version = \"${CURRENT_VERSION}\"/" pyproject.toml
    else
        sed -i "s/^version = \".*\"/version = \"${CURRENT_VERSION}\"/" pyproject.toml
    fi

    echo -e "${GREEN}✅ Version rolled back to ${CURRENT_VERSION}${NC}"
    echo -e "${RED}❌ Publish failed${NC}"
    exit 1
}

# Set up trap to rollback on error or interrupt
trap rollback_version ERR INT TERM

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 1: Updating version in pyproject.toml${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Update version in pyproject.toml (works on both macOS and Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" pyproject.toml
else
    # Linux
    sed -i "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" pyproject.toml
fi

echo -e "${GREEN}✅ Version updated: ${CURRENT_VERSION} → ${NEW_VERSION}${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 2: Syncing dependencies${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

uv sync
echo -e "${GREEN}✅ Dependencies synced${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 3: Running tests${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

uv run pytest tests/unit/ -v
echo -e "${GREEN}✅ Tests passed${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 4: Linting and formatting${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

uv run ruff format .
uv run ruff check .
echo -e "${GREEN}✅ Code formatted and linted${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 5: Type checking${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

uv run pyright src/postgres_mcp/
echo -e "${GREEN}✅ Type checking passed${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 6: Cleaning old builds${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

rm -rf dist/
echo -e "${GREEN}✅ Old builds removed${NC}"

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 7: Building package${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

uv build
echo -e "${GREEN}✅ Package built successfully${NC}"
echo
echo "Build artifacts:"
ls -lh dist/

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 8: Publishing to PyPI${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo

# Ask for publish target
echo "Where to publish?"
echo "  1) PyPI Production (default)"
echo "  2) Test PyPI (recommended for testing)"
echo "  3) Skip publishing"
echo
read -p "Select [1-3] (default: 1): " PUBLISH_TARGET
PUBLISH_TARGET=${PUBLISH_TARGET:-1}

case $PUBLISH_TARGET in
    1)
        echo
        echo -e "${YELLOW}⚠️  About to publish to PRODUCTION PyPI${NC}"
        read -p "Are you sure? [y/N]: " FINAL_CONFIRM
        if [[ ! $FINAL_CONFIRM =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}⚠️  Publish cancelled${NC}"
            rollback_version
        fi
        uv publish
        echo -e "${GREEN}✅ Published to PyPI${NC}"
        # Disable rollback trap after successful publish
        trap - ERR INT TERM
        echo
        echo -e "${GREEN}🎉 Version ${NEW_VERSION} is now live!${NC}"
        echo
        echo "Install with:"
        echo -e "  ${BLUE}uvx acquis-postgres-mcp \"postgresql://...\" --access-mode=unrestricted${NC}"
        ;;
    2)
        uv publish --publish-url https://test.pypi.org/legacy/
        echo -e "${GREEN}✅ Published to Test PyPI${NC}"
        # Disable rollback trap after successful publish
        trap - ERR INT TERM
        echo
        echo "Test with:"
        echo -e "  ${BLUE}uvx --from https://test.pypi.org/simple/ acquis-postgres-mcp \"postgresql://...\"${NC}"
        ;;
    3)
        echo -e "${YELLOW}⚠️  Publishing skipped${NC}"
        # Disable rollback trap if user skips publishing
        trap - ERR INT TERM
        echo
        echo "To publish later, run:"
        echo -e "  ${BLUE}uv publish${NC}"
        ;;
    *)
        echo -e "${RED}❌ Invalid option${NC}"
        exit 1
        ;;
esac

echo
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 9: Git commit (optional)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo
read -p "Commit version bump to git? [y/N]: " GIT_COMMIT
if [[ $GIT_COMMIT =~ ^[Yy]$ ]]; then
    git add pyproject.toml uv.lock
    git commit -m "Bump version to ${NEW_VERSION}"
    echo -e "${GREEN}✅ Changes committed${NC}"
    echo
    read -p "Push to remote? [y/N]: " GIT_PUSH
    if [[ $GIT_PUSH =~ ^[Yy]$ ]]; then
        git push
        echo -e "${GREEN}✅ Changes pushed${NC}"
    fi
fi

echo
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✨ All done!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
