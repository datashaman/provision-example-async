#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
target_arch="${1:-amd64}"
case "$target_arch" in
  amd64|arm64) ;;
  *) echo "usage: $0 [amd64|arm64]" >&2; exit 2 ;;
esac

output_dir="$repo_dir/dist"
mkdir -p "$output_dir"
build_dir="$(mktemp -d)"
trap 'rm -rf "$build_dir"' EXIT

for component in task worker; do
  binary="provision-example-async-$component"
  (cd "$repo_dir" && GOOS=linux GOARCH="$target_arch" CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-buildid=' -o "$build_dir/$binary" "./cmd/$binary")
  chmod 0755 "$build_dir/$binary"
  archive="$output_dir/$binary-linux-$target_arch.tar.gz"
  (cd "$repo_dir" && go run ./cmd/package --input "$build_dir/$binary" --name "$binary" --output "$archive")
done

(
  cd "$output_dir"
  checksum_file="SHA256SUMS"
  : > "$checksum_file"
  for archive in provision-example-async-*-linux-"$target_arch".tar.gz; do
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "$archive" >> "$checksum_file"
    else
      shasum -a 256 "$archive" >> "$checksum_file"
    fi
  done
)

printf 'Release assets:\n'
printf '  %s\n' "$output_dir/provision-example-async-task-linux-$target_arch.tar.gz"
printf '  %s\n' "$output_dir/provision-example-async-worker-linux-$target_arch.tar.gz"
printf '  %s\n' "$output_dir/SHA256SUMS"
