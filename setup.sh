#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/app"
ENV_FILE="$APP_DIR/.env"
COMPOSE_URL="https://raw.githubusercontent.com/Rh8831/Valhallabot/refs/heads/main/docker-compose.yml"
COMPOSE_FILE="$APP_DIR/docker-compose.yml"

mkdir -p "$APP_DIR"
touch "$ENV_FILE"

# ---------- helpers ----------
set_kv () {
  local key="$1"; local val="$2"
  mkdir -p "$APP_DIR"
  [ -f "$ENV_FILE" ] || touch "$ENV_FILE"
  if grep -q "^$key=" "$ENV_FILE"; then
    local tmp="${ENV_FILE}.tmp"
    awk -v k="$key" -v v="$val" 'BEGIN{FS=OFS="="} $1==k{$2=v; print; next} {print}' "$ENV_FILE" > "$tmp"
    mv "$tmp" "$ENV_FILE"
  else
    echo "$key=$val" >> "$ENV_FILE"
  fi
}

get_kv () {
  local key="$1"
  [ -f "$ENV_FILE" ] || { touch "$ENV_FILE"; }
  grep -E "^$key=" "$ENV_FILE" | head -n1 | cut -d= -f2- || true
}

gen_rand () { head -c 64 /dev/urandom | tr -dc 'A-Za-z0-9' | head -c 24; }
gen_user () { echo "user_$(head -c 32 /dev/urandom | tr -dc 'a-z0-9' | head -c 10)"; }

ask_required () {
  local var="$1"; local question="$2"
  local current val=""
  current="$(get_kv "$var")"
  while [ -z "$val" ]; do
    if [ -n "$current" ]; then
      printf "%s [%s]: " "$question" "$current"
    else
      printf "%s: " "$question"
    fi
    IFS= read -r val || true
    if [ -z "$val" ] && [ -n "$current" ]; then val="$current"; fi
    if [ -z "$val" ]; then echo "This value is required."; fi
  done
  set_kv "$var" "$val"
}

ask_db_blank_random () {
  local var="$1"; local question="$2"; local kind="$3"
  local current input=""
  current="$(get_kv "$var")"
  if [ -n "$current" ]; then
    printf "%s [%s] (blank = random): " "$question" "$current"
  else
    printf "%s (blank = random): " "$question"
  fi
  IFS= read -r input || true
  if [ -z "$input" ]; then
    if [ -n "$current" ]; then input="$current"
    else
      if [ "$kind" = "user" ]; then input="$(gen_user)"; else input="$(gen_rand)"; fi
      echo "→ generated: $input"
    fi
  fi
  set_kv "$var" "$input"
}

validate_admins () {
  local val
  val="$(get_kv ADMIN_IDS | tr -d ' ')"
  case "$val" in
    *[!0-9,]*|"")
      echo "ADMIN_IDS must be comma-separated numeric IDs (e.g. 123456789,987654321)."
      ask_required ADMIN_IDS "What are your Telegram admin IDs (comma-separated)"
      validate_admins
      ;;
  esac
}

detect_compose () {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
  elif command -v podman >/dev/null 2>&1 && { podman compose version >/dev/null 2>&1 || podman compose --help >/dev/null 2>&1; }; then
    echo "podman compose"
  elif command -v podman-compose >/dev/null 2>&1; then
    echo "podman-compose"
  else
    echo ""
  fi
}

choose_runtime () {
  local runtime=""
  while [[ "$runtime" != "docker" && "$runtime" != "podman" ]]; do
    read -rp "Choose container runtime (docker/podman): " runtime
    runtime="${runtime,,}"
  done
  echo "$runtime"
}

have_cmd () { command -v "$1" >/dev/null 2>&1; }

is_ubuntu () { [ -f /etc/os-release ] && grep -qi 'ubuntu' /etc/os-release; }
is_debian () { [ -f /etc/os-release ] && grep -qi 'debian' /etc/os-release; }
is_rhel_like () { [ -f /etc/os-release ] && grep -Eqi 'rhel|centos|rocky|alma|oracle' /etc/os-release; }

apt_install () { DEBIAN_FRONTEND=noninteractive apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"; }
dnf_install () { dnf install -y "$@"; }

# ---------- podman-specific helpers ----------
podman_backend () {
  podman info 2>/dev/null | awk -F': ' '/networkBackend:/ {print $2}' | tr -d ' '
}

ensure_mysql_fq_image () {
  # ensure compose uses fully-qualified mysql image when using podman
  if [ -f "$COMPOSE_FILE" ]; then
    sed -i 's~image:[[:space:]]*mysql:8\.0~image: docker.io/library/mysql:8.0~' "$COMPOSE_FILE" || true
  fi
}

ensure_podman_dns () {
  # Ensures container-to-container DNS works for podman
  local backend
  backend="$(podman_backend || true)"
  [ -z "$backend" ] && backend="cni"

  echo "Podman network backend: $backend"

  if [ "$backend" = "netavark" ]; then
    # netavark + aardvark-dns is usually already installed with podman
    if is_ubuntu || is_debian; then
      apt_install netavark aardvark-dns || true
    elif is_rhel_like; then
      dnf_install netavark aardvark-dns || true
    fi
    # recreate the network to ensure proper DNS
    podman network rm app_default >/dev/null 2>&1 || true
    podman network create app_default >/dev/null
    return 0
  fi

  # CNI path: need dnsname plugin present AND in conflist
  echo "Ensuring CNI dnsname plugin is installed…"
  local dnsname_ok=""
  for p in /opt/cni/bin/dnsname /usr/lib/cni/dnsname /usr/libexec/cni/dnsname; do
    [ -x "$p" ] && dnsname_ok="yes"
  done
  if [ -z "$dnsname_ok" ]; then
    if is_ubuntu || is_debian; then
      # core plugins + dnsname; package names vary by distro build
      apt_install containernetworking-plugins || true
      apt_install golang-github-containernetworking-plugin-dnsname || apt_install cni-plugin-dnsname || true
      apt_install dnsmasq-base || true
    elif is_rhel_like; then
      dnf_install containernetworking-plugins podman-plugins dnsmasq || true
    fi
  fi

  echo "Recreating network and adding dnsname to conflist…"
  podman network rm app_default >/dev/null 2>&1 || true
  podman network create app_default >/dev/null

  local CNI_FILE
  CNI_FILE="$(ls -1 /etc/cni/net.d/*app_default*.conflist 2>/dev/null | head -n1 || true)"
  if [ -z "${CNI_FILE:-}" ]; then
    echo "Could not find CNI conflist for app_default; listing /etc/cni/net.d for debug:"
    ls -l /etc/cni/net.d/ || true
    return 0
  fi

  # If dnsname already present, skip
  if grep -q '"type"[[:space:]]*:[[:space:]]*"dnsname"' "$CNI_FILE"; then
    echo "dnsname already present in $CNI_FILE"
  else
    # Append dnsname plugin to the plugins array
    if ! have_cmd jq; then
      if is_ubuntu || is_debian; then apt_install jq; else dnf_install jq; fi
    fi
    cp "$CNI_FILE" "${CNI_FILE}.bak"
    jq '.plugins += [{"type":"dnsname","domainName":"dns.podman","capabilities":{"aliases":true}}]' \
      "${CNI_FILE}.bak" > "$CNI_FILE"
    echo "Patched $CNI_FILE with dnsname plugin."
  fi
}

# ---------- preflight ----------
RUNTIME="$(choose_runtime)"
if ! have_cmd "$RUNTIME"; then
  echo "$RUNTIME is not installed."
  read -rp "Install $RUNTIME now? [y/N]: " reply
  reply="${reply,,}"
  if [[ "$reply" == "y" || "$reply" == "yes" ]]; then
    if is_ubuntu || is_debian; then apt_install "$RUNTIME"; elif is_rhel_like; then dnf_install "$RUNTIME"; else echo "Unknown distro; install $RUNTIME manually."; exit 1; fi
  else
    echo "Aborting."; exit 1
  fi
fi

# ---------- defaults ----------
[ -n "$(get_kv MYSQL_HOST)" ] || set_kv "MYSQL_HOST" "mysql"
[ -n "$(get_kv MYSQL_PORT)" ] || set_kv "MYSQL_PORT" "3306"
[ -n "$(get_kv MYSQL_DATABASE)" ] || set_kv "MYSQL_DATABASE" "valhalla"
[ -n "$(get_kv FLASK_HOST)" ] || set_kv "FLASK_HOST" "0.0.0.0"
[ -n "$(get_kv FLASK_PORT)" ] || set_kv "FLASK_PORT" "5000"
[ -n "$(get_kv WORKERS)" ] || set_kv "WORKERS" "$((2 * $(nproc 2>/dev/null || echo 1) + 1))"
[ -n "$(get_kv USAGE_SYNC_INTERVAL)" ] || set_kv "USAGE_SYNC_INTERVAL" "60"
[ -n "$(get_kv IMAGE)" ] || set_kv "IMAGE" "ghcr.io/rh8831/valhalla:latest"

# ---------- ask user ----------
echo "---- Telegram ----"
ask_required "BOT_TOKEN" "What is your Telegram bot token"
ask_required "ADMIN_IDS" "What are your Telegram admin IDs (comma-separated)"
validate_admins

ask_required "PUBLIC_BASE_URL" "What is your public base URL (e.g. https://example.com)"

ask_required "FLASK_PORT" "Which port should the app listen on"
if [ "$(get_kv FLASK_PORT)" = "443" ]; then
  ask_required "SSL_DOMAIN" "What domain should be used for HTTPS"
  domain="$(get_kv SSL_DOMAIN)"
  echo "Obtaining SSL certificate for $domain ..."
  mkdir -p "$APP_DIR/certs"
  # stop anything on :80 first (best effort)
  (have_cmd systemctl && sudo systemctl stop nginx apache2 httpd 2>/dev/null) || true
  $RUNTIME run --rm -p 80:80 -v "$APP_DIR/certs:/etc/letsencrypt" \
    docker.io/certbot/certbot certonly --standalone --non-interactive \
    --agree-tos --register-unsafely-without-email -d "$domain" || true
  set_kv "SSL_CERT_PATH" "/app/certs/live/$domain/fullchain.pem"
  set_kv "SSL_KEY_PATH" "/app/certs/live/$domain/privkey.pem"
fi

echo "---- MySQL (blank = random) ----"
ask_db_blank_random "MYSQL_USER" "MySQL app username" "user"
ask_db_blank_random "MYSQL_PASSWORD" "MySQL app password" "pass"
ask_db_blank_random "MYSQL_ROOT_PASSWORD" "MySQL ROOT password" "root"

echo "✓ Saved to $ENV_FILE."

# ---------- fetch docker-compose.yml ----------
if [ ! -f "$COMPOSE_FILE" ]; then
  echo "Fetching docker-compose.yml from $COMPOSE_URL ..."
  curl -fsSL "$COMPOSE_URL" -o "$COMPOSE_FILE"
  echo "Saved as $COMPOSE_FILE"
fi

# If runtime is podman, ensure FQ mysql image + working DNS
if [ "$RUNTIME" = "podman" ]; then
  ensure_mysql_fq_image
  ensure_podman_dns
fi

# ---------- start services ----------
COMPOSE_BIN="$(detect_compose || true)"
if [ -n "$COMPOSE_BIN" ] && [ -f "$COMPOSE_FILE" ]; then
  echo "Pulling images with $COMPOSE_BIN -f $COMPOSE_FILE pull ..."
  $COMPOSE_BIN -f "$COMPOSE_FILE" pull
  echo "Starting services with $COMPOSE_BIN -f $COMPOSE_FILE up -d --no-build ..."
  $COMPOSE_BIN -f "$COMPOSE_FILE" up -d --no-build
  echo "Done. Use '$COMPOSE_BIN -f $COMPOSE_FILE logs -f' to follow logs."

  # quick post-check (best effort)
  if [ "$RUNTIME" = "podman" ]; then
    echo "Verifying container-to-container DNS (mysql)…"
    ( podman exec -it valhalla-app getent hosts mysql >/dev/null 2>&1 && echo "DNS OK" ) || echo "DNS check failed (run: podman exec -it valhalla-app getent hosts mysql)"
  fi
else
  echo "compose not found or $COMPOSE_FILE missing. Start services manually later."
fi
