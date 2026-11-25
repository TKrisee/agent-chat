#!/usr/bin/env bash
# Call only after verification of the exact source and all three native package jobs.
set -euo pipefail
: "${GH_REPO:?}" "${TAG:?}" "${SHA:?}" "${WORKFLOW_SHA:?}"
PREVIOUS=${PREVIOUS:-}
[[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ && "$SHA" =~ ^[0-9a-f]{40}$ && "$WORKFLOW_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ -z "$PREVIOUS" || "$PREVIOUS" =~ ^[0-9a-f]{40}$ ]]
[[ "$(gh api "repos/$GH_REPO/git/ref/heads/main" | jq -er '.object.sha')" == "$WORKFLOW_SHA" ]]
[[ "$(git show "$SHA:.release-please-manifest.json" | jq -er '.["."]')" == "${TAG#v}" ]]
if [[ -n "$PREVIOUS" ]]; then
  [[ "$PREVIOUS" != "$SHA" ]]
  gh api "repos/$GH_REPO/compare/$PREVIOUS...$SHA" | jq -e --arg old "$PREVIOUS" '.status == "ahead" and .merge_base_commit.sha == $old' >/dev/null
fi
gh api "repos/$GH_REPO/git/matching-refs/tags/$TAG" | jq -e --arg ref "refs/tags/$TAG" '[.[] | select(.ref == $ref)] | length == 0' >/dev/null
for file in agentchat-linux-amd64.tar.gz agentchat-darwin-arm64.tar.gz agentchat-windows-amd64.zip; do
  test -f "artifacts/$file"
  expected=$(cut -d' ' -f1 "artifacts/$file.sha256")
  [[ "$expected" =~ ^[0-9a-f]{64}$ && "$(sha256sum "artifacts/$file" | cut -d' ' -f1)" == "$expected" ]]
done
printf 'version %s\n' "${TAG#v}" > artifacts/agentchat-update.txt
for file in agentchat-linux-amd64.tar.gz agentchat-darwin-arm64.tar.gz agentchat-windows-amd64.zip; do
  printf '%s %s\n' "$(sha256sum "artifacts/$file" | cut -d' ' -f1)" "$file" >> artifacts/agentchat-update.txt
done
# Generate only the exact release section from the tested source.
git show "$SHA:CHANGELOG.md" | awk -v header="## [${TAG#v}]" 'index($0,header)==1 {found=1;next} found && /^## / {exit} found {print}' > "$RUNNER_TEMP/release-notes.md"
test -s "$RUNNER_TEMP/release-notes.md"
gh api --paginate "repos/$GH_REPO/releases?per_page=100" | jq -s --arg tag "$TAG" '[.[][] | select(.tag_name == $tag) | {id,draft,target_commitish}]' > "$RUNNER_TEMP/release-metadata.json"
jq -c '.' "$RUNNER_TEMP/release-metadata.json"
count=$(jq -er 'length' "$RUNNER_TEMP/release-metadata.json")
if [[ "$count" == 0 ]]; then
  id=$(gh api --method POST "repos/$GH_REPO/releases" -f tag_name="$TAG" -f name="$TAG" -f target_commitish="$SHA" -F draft=true -F "body=@$RUNNER_TEMP/release-notes.md" | jq -er --arg sha "$SHA" --arg tag "$TAG" 'select(.draft == true and .tag_name == $tag and .target_commitish == $sha) | .id')
elif [[ "$count" == 1 ]]; then
  id=$(jq -er --arg old "$PREVIOUS" --arg sha "$SHA" '.[0] | select(.draft == true and (.target_commitish == $sha or ($old != "" and .target_commitish == $old))) | .id' "$RUNNER_TEMP/release-metadata.json")
  if [[ -n "$PREVIOUS" ]]; then
    gh api --method PATCH "repos/$GH_REPO/releases/$id" -f target_commitish="$SHA" | jq -e --arg sha "$SHA" --arg tag "$TAG" '.draft == true and .tag_name == $tag and .target_commitish == $sha' >/dev/null
  fi
else
  echo 'Ambiguous release identity; refusing publication' >&2; exit 1
fi
[[ "$(gh api "repos/$GH_REPO/git/ref/heads/main" | jq -er '.object.sha')" == "$WORKFLOW_SHA" ]]
(cd artifacts && gh release upload "$TAG" agentchat-update.txt agentchat-linux-amd64.tar.gz agentchat-darwin-arm64.tar.gz agentchat-windows-amd64.zip --clobber)
[[ "$(gh api "repos/$GH_REPO/git/ref/heads/main" | jq -er '.object.sha')" == "$WORKFLOW_SHA" ]]
gh api "repos/$GH_REPO/git/matching-refs/tags/$TAG" | jq -e --arg ref "refs/tags/$TAG" '[.[] | select(.ref == $ref)] | length == 0' >/dev/null
gh api --method PATCH "repos/$GH_REPO/releases/$id" -F draft=false | jq -e --arg sha "$SHA" --arg tag "$TAG" '.draft == false and .tag_name == $tag and .target_commitish == $sha' >/dev/null
