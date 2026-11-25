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
gh api --paginate "repos/$GH_REPO/releases?per_page=100" | jq -s --arg tag "$TAG" '[.[][] | select(.tag_name == $tag or (.draft == true and (.tag_name | startswith("untagged-")) and .name == $tag)) | {id,draft,target_commitish,tag_name,name}]' > "$RUNNER_TEMP/release-metadata.json"
jq -c '.' "$RUNNER_TEMP/release-metadata.json"
count=$(jq -er 'length' "$RUNNER_TEMP/release-metadata.json")
untagged=false
if [[ "$count" == 0 ]]; then
  gh api --method POST "repos/$GH_REPO/releases" -f tag_name="$TAG" -f name="$TAG" -f target_commitish="$SHA" -F draft=true -F "body=@$RUNNER_TEMP/release-notes.md" > "$RUNNER_TEMP/new-release.json"
  id=$(jq -er --arg sha "$SHA" --arg tag "$TAG" 'select(.draft == true and .target_commitish == $sha and (.tag_name == $tag or ((.tag_name | startswith("untagged-")) and .name == $tag))) | .id' "$RUNNER_TEMP/new-release.json")
  untagged=$(jq -r '.tag_name | startswith("untagged-")' "$RUNNER_TEMP/new-release.json")
elif [[ "$count" == 1 ]]; then
  id=$(jq -er --arg old "$PREVIOUS" --arg sha "$SHA" '.[0] | select(.draft == true and (.target_commitish == $sha or ($old != "" and .target_commitish == $old))) | .id' "$RUNNER_TEMP/release-metadata.json")
  untagged=$(jq -r '.[0].tag_name | startswith("untagged-")' "$RUNNER_TEMP/release-metadata.json")
  if [[ -n "$PREVIOUS" ]]; then
    gh api --method PATCH "repos/$GH_REPO/releases/$id" -f target_commitish="$SHA" | jq -e --arg sha "$SHA" --arg tag "$TAG" '.draft == true and .target_commitish == $sha and (.tag_name == $tag or ((.tag_name | startswith("untagged-")) and .name == $tag))' >/dev/null
  fi
else
  echo 'Ambiguous release identity; refusing publication' >&2; exit 1
fi
[[ "$id" =~ ^[0-9]+$ ]]
[[ "$(gh api "repos/$GH_REPO/git/ref/heads/main" | jq -er '.object.sha')" == "$WORKFLOW_SHA" ]]
if [[ "$untagged" == true ]]; then
  upload_base="https://uploads.github.com/repos/$GH_REPO/releases/$id/assets"
  gh api "repos/$GH_REPO/releases/$id" | jq -e --arg sha "$SHA" --arg tag "$TAG" --arg upload "$upload_base{?name,label}" --argjson id "$id" '.id == $id and .draft == true and .target_commitish == $sha and .name == $tag and (.tag_name | startswith("untagged-")) and .upload_url == $upload' >/dev/null
  for file in agentchat-update.txt agentchat-linux-amd64.tar.gz agentchat-darwin-arm64.tar.gz agentchat-windows-amd64.zip; do
    asset_id=$(gh api --paginate "repos/$GH_REPO/releases/$id/assets?per_page=100" | jq -ers --arg name "$file" '[.[][] | select(.name == $name) | .id] | if length == 0 then "" elif length == 1 then .[0] else error("ambiguous asset identity") end')
    if [[ -n "$asset_id" ]]; then
      gh api --method DELETE "repos/$GH_REPO/releases/assets/$asset_id" >/dev/null
    fi
    gh api --method POST "$upload_base?name=$file" -H 'Content-Type: application/octet-stream' --input "artifacts/$file" | jq -e --arg name "$file" '.name == $name and .state == "uploaded"' >/dev/null
  done
else
  (cd artifacts && gh release upload "$TAG" agentchat-update.txt agentchat-linux-amd64.tar.gz agentchat-darwin-arm64.tar.gz agentchat-windows-amd64.zip --clobber)
fi
[[ "$(gh api "repos/$GH_REPO/git/ref/heads/main" | jq -er '.object.sha')" == "$WORKFLOW_SHA" ]]
gh api "repos/$GH_REPO/git/matching-refs/tags/$TAG" | jq -e --arg ref "refs/tags/$TAG" '[.[] | select(.ref == $ref)] | length == 0' >/dev/null
gh api --method PATCH "repos/$GH_REPO/releases/$id" -f tag_name="$TAG" -f target_commitish="$SHA" -F draft=false | jq -e --arg sha "$SHA" --arg tag "$TAG" '.draft == false and .tag_name == $tag and .target_commitish == $sha' >/dev/null
