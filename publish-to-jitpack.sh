#!/bin/bash

# Script to publish the fork to JitPack
# Run this script from the repository root

echo "=== Step 1: Check current branch ==="
git branch --show-current

echo ""
echo "=== Step 2: Stage all changes ==="
git add .

echo ""
echo "=== Step 3: Show what will be committed ==="
git status

echo ""
echo "=== Step 4: Commit changes ==="
git commit -m "fix: JitPack multi-module publishing configuration"

echo ""
echo "=== Step 5: Delete old tag locally ==="
git tag -d 1.24.1-custom 2>/dev/null || echo "Local tag doesn't exist"

echo ""
echo "=== Step 6: Delete old tag on remote ==="
git push origin :refs/tags/1.24.1-custom 2>/dev/null || echo "Remote tag doesn't exist"

echo ""
echo "=== Step 7: Create new tag ==="
git tag -a 1.24.1-custom -m "Release 1.24.1-custom with JitPack support"

echo ""
echo "=== Step 8: Push commits ==="
git push origin HEAD

echo ""
echo "=== Step 9: Push new tag ==="
git push origin 1.24.1-custom

echo ""
echo "=== DONE ==="
echo "Now go to https://jitpack.io/#omarH2007/analytics-kotlin-fork/1.24.1-custom"
echo "Click 'Get it' to trigger a new build"

