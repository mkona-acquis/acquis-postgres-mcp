#!/bin/bash
# Quick publish script - assumes tests pass, just bumps patch version and publishes
set -e

CURRENT_VERSION=$(grep -E '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"
NEW_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))"

echo "🚀 Quick publish: ${CURRENT_VERSION} → ${NEW_VERSION}"
echo

# Rollback function
rollback_version() {
    echo
    echo "⚠️  Error detected - Rolling back version"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/^version = \".*\"/version = \"${CURRENT_VERSION}\"/" pyproject.toml
    else
        sed -i "s/^version = \".*\"/version = \"${CURRENT_VERSION}\"/" pyproject.toml
    fi

    echo "✅ Version rolled back to ${CURRENT_VERSION}"
    echo "❌ Publish failed"
    exit 1
}

# Set up trap to rollback on error or interrupt
trap rollback_version ERR INT TERM

# Update version
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" pyproject.toml
else
    sed -i "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" pyproject.toml
fi

# Build and publish
uv sync
rm -rf dist/
uv build
uv publish

# Disable rollback trap after successful publish
trap - ERR INT TERM

echo
echo "✅ Published version ${NEW_VERSION}"
echo "Install with: uvx acquis-postgres-mcp \"postgresql://...\""
