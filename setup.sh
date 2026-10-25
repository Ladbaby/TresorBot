#!/usr/bin/env bash
# Install Sun, Earth, or both from a TresorBot GitHub release.
# curl -fsSL https://raw.githubusercontent.com/Ladbaby/TresorBot/main/setup.sh | bash -s -- sun
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: setup.sh sun|earth|both [--prerelease]' \
        'Installs to ~/.local/bin; preserves ~/.config/tresorbot/*.yaml.' \
        'Set TRESORBOT_VERSION=v0.1.0 to pin a release.' \
        'Set TRESORBOT_PRERELEASE=true to include pre-releases.'
}
fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }
role=''
prerelease=${TRESORBOT_PRERELEASE:-false}
while [[ $# -gt 0 ]]; do
    case "$1" in
        sun|earth|both) [[ -z "$role" ]] || fail 'Choose only one role.'; role=$1 ;;
        --prerelease) prerelease=true ;;
        --help|-h) usage; exit 0 ;;
        *) fail "Unknown argument: $1" ;;
    esac
    shift
done
[[ -n "$role" ]] || { usage >&2; exit 1; }
case "$(uname -s)" in
    Linux) platform=linux ;;
    Darwin) platform=darwin ;;
    *) fail 'This script supports Linux and macOS. On Windows use setup.ps1.' ;;
esac
case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) fail 'Supported architectures: amd64 and arm64.' ;;
esac
command -v gzip >/dev/null || fail 'gzip is required.'
if command -v curl >/dev/null; then
    download() { curl -fsSL --retry 2 -o "$2" "$1"; }
elif command -v wget >/dev/null; then
    download() { wget -q -O "$2" "$1"; }
else
    fail 'curl or wget is required.'
fi
if command -v sha256sum >/dev/null; then
    hash() { sha256sum "$1"; }
elif command -v shasum >/dev/null; then
    hash() { shasum -a 256 "$1"; }
else
    fail 'sha256sum or shasum is required for download verification.'
fi
repo=https://github.com/Ladbaby/TresorBot
api=https://api.github.com/repos/Ladbaby/TresorBot
bin_dir=$HOME/.local/bin
config_dir=$HOME/.config/tresorbot
work=$(mktemp -d "${TMPDIR:-/tmp}/tresorbot-setup.XXXXXX")
trap 'rm -rf "$work"' EXIT
version=${TRESORBOT_VERSION:-}
if [[ -z "$version" ]]; then
    endpoint=$api/releases/latest
    [[ "$prerelease" != true ]] || endpoint=$api/releases
    download "$endpoint" "$work/release.json" || fail 'Cannot fetch release information.'
    # Like Tresor's installer, use the first tag from GitHub's release response.
    # Public release discovery excludes drafts; /releases also includes prereleases.
    release_info=$(< "$work/release.json")
    tag_pattern='"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)"'
    [[ "$release_info" =~ $tag_pattern ]] || fail 'Cannot determine release version.'
    version=${BASH_REMATCH[1]}
fi
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]] || fail 'Invalid release version; use v0.1.0 or v0.1.0-rc.1.'
base=$repo/releases/download/$version
download "$base/checksums.txt" "$work/checksums.txt" || fail 'Cannot download required checksums.'
verify() {
    local name=$1 expected='' count=0 checksum filename actual ignored
    while read -r checksum filename ignored; do
        filename=${filename%$'\r'}
        filename=${filename#\*}
        if [[ "$filename" == "$name" || "$filename" == "./$name" ]]; then
            [[ -z "$ignored" ]] || fail "Invalid checksum entry for $name."
            expected=$checksum
            count=$((count + 1))
        fi
    done < "$work/checksums.txt"
    [[ "$count" == 1 && "$expected" =~ ^[0-9a-fA-F]{64}$ ]] || fail "Missing or ambiguous checksum for $name."
    read -r actual ignored < <(hash "$work/$name")
    actual=$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')
    expected=$(printf '%s' "$expected" | tr '[:upper:]' '[:lower:]')
    [[ "$actual" == "$expected" ]] || fail "Checksum mismatch for $name."
}
roles=("$role")
[[ "$role" != both ]] || roles=(sun earth)
# Complete all downloads and validation before replacing any installed files.
for component in "${roles[@]}"; do
    name=tresorbot-$component-$platform-$arch-$version.gz
    printf 'Downloading %s\n' "$name"
    download "$base/$name" "$work/$name" || fail "Cannot download $name."
    verify "$name"
    gzip -dc "$work/$name" > "$work/tresorbot-$component" || fail "Cannot extract $name."
    [[ -s "$work/tresorbot-$component" ]] || fail "Empty binary: $name."
    if [[ ! -e "$config_dir/$component.yaml" ]]; then
        example=$component.example.yaml
        download "$base/$example" "$work/$example" || fail "Cannot download $example."
        verify "$example"
        [[ -s "$work/$example" ]] || fail "Empty example config: $example."
    fi
done
mkdir -p "$bin_dir" "$config_dir"
for component in "${roles[@]}"; do
    target=$bin_dir/tresorbot-$component
    if [[ -e "$target" ]]; then
        cp "$target" "$target.bak"
        printf 'Backup: %s.bak\n' "$target"
    fi
    cp "$work/tresorbot-$component" "$target"
    chmod +x "$target"
    if [[ ! -e "$config_dir/$component.yaml" ]]; then
        cp "$work/$component.example.yaml" "$config_dir/$component.yaml"
    fi
    printf 'Installed %s (%s)\nConfig: %s/%s.yaml (existing settings preserved)\n' "$target" "$version" "$config_dir" "$component"
done
if [[ ":$PATH:" != *":$bin_dir:"* ]]; then
    # shellcheck disable=SC2016 # Print a literal $PATH for the user's profile.
    printf '\nAdd this to your shell profile:\n  export PATH="%s:$PATH"\n' "$bin_dir"
fi
printf '\nEdit the config before starting. No services have been started.\n'
for component in "${roles[@]}"; do
    if [[ "$component" == sun ]]; then
        printf 'Sun: choose your owner password and tokens, then run:\n'
    else
        printf 'Earth: install Node.js and Pi Agent, enroll this Earth in Sun, then review Earth config. Run:\n'
    fi
    printf '  "%s/tresorbot-%s" --config "%s/%s.yaml"\n' "$bin_dir" "$component" "$config_dir" "$component"
done
printf 'Sun dashboard: http://127.0.0.1:11520\nConnect Tresor using the README: https://github.com/Ladbaby/TresorBot#connect-tresor\n'
