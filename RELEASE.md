# Releasing buildkite-logs

Releases are published manually with the GitHub CLI (`gh`), using the `bklog`
binaries built by CI. Use Bash for the commands below, with `gh` authenticated
to an account that can create releases in this repository.

Attach and verify every binary asset **while the release is a draft**, then
publish it. Consumers download these assets by name and may pin their SHA-256
checksums, so never replace the assets of a published release; release a new
version instead.

## 1. Prepare the version and build

1. Open and merge a PR updating [`VERSION`](VERSION) to the next version. Use a
   `0.x` minor bump for breaking API changes and a patch bump for compatible
   fixes (see [`AGENTS.md`](AGENTS.md)). Do not reuse a published version.
2. From a clean checkout, update `main` and record the release version and
   commit:

   ```bash
   git switch main
   git pull --ff-only
   VERSION=$(make version)
   TAG="v$VERSION"
   COMMIT=$(git rev-parse HEAD)
   printf 'Release %s from %s\n' "$TAG" "$COMMIT"
   ```

3. Wait for the CI build of `$COMMIT` on `main` to pass. Its **dist** step
   uploads the release binaries as build artifacts.

## 2. Download and verify the artifacts

Download the **dist** step's artifacts into `dist/`, keeping their filenames
unchanged. Use the CI artifacts rather than rebuilding locally. The expected
filenames are defined by [`Makefile`](Makefile):

```bash
ASSETS=(
  "dist/bklog-$VERSION-linux-amd64.gz"
  "dist/bklog-$VERSION-linux-arm64.gz"
  "dist/bklog-$VERSION-darwin-amd64.gz"
  "dist/bklog-$VERSION-darwin-arm64.gz"
  "dist/bklog-$VERSION-sha256sums.txt"
)

for asset in "${ASSETS[@]}"; do
  test -s "$asset" || { echo "Missing or empty asset: $asset"; exit 1; }
done

(cd dist && shasum -a 256 -c "bklog-$VERSION-sha256sums.txt")
for archive in dist/*.gz; do gzip -t "$archive"; done
```

Extract the binary for your platform and run it with `version`; it must report
`bklog version $VERSION`.

## 3. Create and review a draft

Create the draft with all assets attached, using GitHub's generated notes like
earlier releases. `--target` pins the release, and the tag it creates on
publishing, to the verified commit:

```bash
gh release create "$TAG" "${ASSETS[@]}" \
  --draft \
  --target "$COMMIT" \
  --title "$TAG" \
  --generate-notes

gh release view "$TAG" --json isDraft,targetCommitish,body,assets,url
```

Confirm that `isDraft` is `true`, the target commit is correct, and all five
assets have the expected names and sizes. Download the attached assets and
check them against the checksum file again. If an upload fails, use
`gh release upload "$TAG"` with only the missing paths while the release is
still a draft.

## 4. Publish

```bash
gh release edit "$TAG" --draft=false
gh release view "$TAG" --json isDraft,publishedAt,assets,url
```

Confirm the release is published and still lists all five assets.
