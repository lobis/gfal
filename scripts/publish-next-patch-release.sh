#!/usr/bin/env bash

set -euo pipefail

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "error: not inside a git repository" >&2
  exit 1
fi

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

current_branch=$(git branch --show-current)
if [[ "$current_branch" != "main" ]]; then
  echo "error: releases must be published from 'main' (current: '$current_branch')" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is not clean" >&2
  exit 1
fi

git fetch origin main --tags

latest_tag=$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-version:refname | head -n 1)
if [[ -z "$latest_tag" ]]; then
  echo "error: could not find an existing semantic version tag" >&2
  exit 1
fi

version=${latest_tag#v}
IFS=. read -r major minor patch <<<"$version"
next_tag="v${major}.${minor}.$((patch + 1))"

if git rev-parse "$next_tag" >/dev/null 2>&1; then
  echo "error: tag '$next_tag' already exists" >&2
  exit 1
fi

head_commit=$(git rev-parse HEAD)
remote_main=$(git rev-parse origin/main)

if [[ "$head_commit" != "$remote_main" ]]; then
  echo "error: HEAD ($head_commit) does not match origin/main ($remote_main)" >&2
  echo "push main first so the release tag points at the published commit" >&2
  exit 1
fi

git tag -a "$next_tag" -m "Release $next_tag"
git push origin "$next_tag"

echo "$next_tag"
