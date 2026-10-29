#!/bin/bash

# ./misc_scripts/add_filename_comment.sh
# Usage: ./add_filename_comment.sh <directory> [ext1 ext2 ...]
# Example: ./add_filename_comment.sh beaconing-poc-manifests yaml json sh
# Defaults to yaml if no extensions are provided

TARGET_DIR="${1:-.}"
shift

# Default to yaml if no extensions provided
if [ "$#" -eq 0 ]; then
  EXTENSIONS=("yaml")
else
  EXTENSIONS=("$@")
fi

# Build the find command dynamically to support multiple extensions
find_args=("$TARGET_DIR" -type f)

# Add extension filters using -o (OR) logic wrapped in parentheses
find_args+=("(")
for i in "${!EXTENSIONS[@]}"; do
  if [ "$i" -gt 0 ]; then
    find_args+=("-o")
  fi
  find_args+=("-name" "*.${EXTENSIONS[$i]}")
done
find_args+=(")")

find "${find_args[@]}" | while read -r filepath; do
  first_line=$(head -n 1 "$filepath")
  if [[ "$first_line" == "# ${filepath}" ]]; then
    echo "Skipping (already has comment): $filepath"
    continue
  fi

  tmpfile=$(mktemp)
  echo "# ${filepath}" | cat - "$filepath" > "$tmpfile" && mv "$tmpfile" "$filepath"
  echo "Updated: $filepath"
done