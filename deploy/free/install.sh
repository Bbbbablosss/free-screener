#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/free-screener
if [[ "$(pwd)" != "$APP_DIR" ]]; then
  echo "Run this installer from $APP_DIR" >&2
  exit 2
fi

install -d -m 0750 "$APP_DIR/runtime/redis" "$APP_DIR/backend/data"

python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install --upgrade pip
"$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt"

(
  cd "$APP_DIR/goingest"
  go build -trimpath -o goingest .
)

install -m 0644 deploy/free/free-screener-redis.service /etc/systemd/system/
install -m 0644 deploy/free/free-screener-web.service /etc/systemd/system/
install -m 0644 deploy/free/free-screener-gateway.service /etc/systemd/system/
install -m 0644 deploy/free/free-screener-ingest@.service /etc/systemd/system/
install -m 0644 deploy/free/nginx-free.conf /etc/nginx/sites-available/free-screener-8080
ln -sfn /etc/nginx/sites-available/free-screener-8080 /etc/nginx/sites-enabled/free-screener-8080

systemctl daemon-reload
systemctl enable --now free-screener-redis.service
systemctl enable --now free-screener-web.service
systemctl enable --now free-screener-gateway.service
for role in klines-core klines-alt oi funding trades metrics detectors; do
  systemctl enable --now "free-screener-ingest@${role}.service"
done

nginx -t
systemctl reload nginx

echo "Free Screener is installed. Verify locally and then open http://SERVER_IP:8080/"

