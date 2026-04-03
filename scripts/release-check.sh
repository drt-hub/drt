#!/usr/bin/env bash
# release-check.sh — verify version consistency and doc freshness before release
set -euo pipefail

FAIL=0
VERSION=$(grep '^version' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
echo "Checking release consistency for v${VERSION}"
echo "================================================"

# 1. Version consistency
check_version() {
    local file=$1
    local pattern=$2
    if grep -q "$pattern" "$file"; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file — expected version $VERSION"
        FAIL=1
    fi
}

echo ""
echo "Version consistency:"
check_version "pyproject.toml" "version = \"${VERSION}\""
check_version ".claude-plugin/plugin.json" "\"version\": \"${VERSION}\""
check_version ".claude-plugin/marketplace.json" "\"version\": \"${VERSION}\""
check_version "skills/drt/.claude-plugin/plugin.json" "\"version\": \"${VERSION}\""

# 2. CHANGELOG entry
echo ""
echo "CHANGELOG:"
if grep -q "## \[${VERSION}\]" CHANGELOG.md; then
    echo "  ✓ CHANGELOG.md has entry for ${VERSION}"
else
    echo "  ✗ CHANGELOG.md missing entry for ${VERSION}"
    FAIL=1
fi

# 3. CLAUDE.md version
echo ""
echo "CLAUDE.md:"
if grep -q "v${VERSION}" CLAUDE.md; then
    echo "  ✓ CLAUDE.md references v${VERSION}"
else
    echo "  ✗ CLAUDE.md does not reference v${VERSION}"
    FAIL=1
fi

# 4. docs/llm/CONTEXT.md version
echo ""
echo "LLM docs:"
if grep -q "v${VERSION}" docs/llm/CONTEXT.md; then
    echo "  ✓ docs/llm/CONTEXT.md references v${VERSION}"
else
    echo "  ✗ docs/llm/CONTEXT.md does not reference v${VERSION}"
    FAIL=1
fi

# 5. SECURITY.md version
echo ""
echo "SECURITY.md:"
MAJOR_MINOR=$(echo "$VERSION" | sed 's/\.[0-9]*$//')
if grep -q "${MAJOR_MINOR}" SECURITY.md; then
    echo "  ✓ SECURITY.md lists ${MAJOR_MINOR}.x as supported"
else
    echo "  ✗ SECURITY.md missing ${MAJOR_MINOR}.x"
    FAIL=1
fi

# 6. Skills sync
echo ""
echo "Skills sync:"
make check-skills 2>&1 | sed 's/^/  /' || FAIL=1

# 7. Summary
echo ""
echo "================================================"
if [ $FAIL -eq 0 ]; then
    echo "✓ All release checks passed"
else
    echo "✗ Some checks failed — fix before releasing"
    exit 1
fi
