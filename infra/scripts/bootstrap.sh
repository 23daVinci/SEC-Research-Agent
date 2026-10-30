#!/usr/bin/env bash
# Creates infra/.env from .env.example and fills in generated secrets.
# Safe to re-run: values that are already set are never changed. This matters for
# AIRFLOW_FERNET_KEY, because changing it makes already-stored connections unreadable.
# Also creates the dags/, plugins/ and logs/ folders that Airflow bind-mounts.
set -euo pipefail

infra_dir="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
example="$infra_dir/.env.example"
env_file="$infra_dir/.env"

[ -f "$example" ] || { echo "Missing $example" >&2; exit 1; }

hex()    { openssl rand -hex "$1"; }
fernet() { openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'; }

generate() {
  case "$1" in
    POSTGRES_PASSWORD|GRAFANA_ADMIN_PASSWORD) hex 12 ;;
    AIRFLOW_FERNET_KEY)                       fernet ;;
    AIRFLOW_API_SECRET_KEY|AIRFLOW_JWT_SECRET) hex 32 ;;
    *) return 1 ;;
  esac
}

if [ -f "$env_file" ]; then
  source_file="$env_file"; echo "$env_file exists; only filling empty values"
else
  source_file="$example"; echo "Creating $env_file from .env.example"
fi

tmp="$(mktemp)"
generated=()
while IFS= read -r line || [ -n "$line" ]; do
  if [[ "$line" =~ ^([A-Z0-9_]+)=(.*)$ ]]; then
    key="${BASH_REMATCH[1]}"; value="${BASH_REMATCH[2]}"
    if [[ -z "$value" || "$value" == "change-me" ]] && secret="$(generate "$key")"; then
      line="$key=$secret"; generated+=("$key")
    fi
  fi
  printf '%s\n' "$line"
done < "$source_file" > "$tmp"
mv "$tmp" "$env_file"

for k in ${generated[@]+"${generated[@]}"}; do echo "  generated $k"; done
[ ${#generated[@]} -eq 0 ] && echo "  nothing to generate"

repo_root="$(dirname "$infra_dir")"
mkdir -p "$repo_root/dags" "$repo_root/plugins" "$repo_root/logs"
echo "Done. Next: docker compose up -d --wait   (from the infra/ folder)"
