#!/bin/bash
set -e

APP_DIR="/opt/screener"
REPO_URL=""   # заполнить перед запуском, либо передать как аргумент: ./setup.sh https://github.com/...
PYTHON="python3.12"

if [ -n "$1" ]; then REPO_URL="$1"; fi
if [ -z "$REPO_URL" ]; then echo "Usage: ./setup.sh <git-repo-url>"; exit 1; fi

echo "=== 1. System packages ==="
apt-get update -qq
apt-get install -y -qq git python3.12 python3.12-venv python3-pip nginx certbot python3-certbot-nginx

echo "=== 2. Clone / update repo ==="
if [ -d "$APP_DIR/.git" ]; then
    git -C "$APP_DIR" pull
else
    git clone "$REPO_URL" "$APP_DIR"
fi

echo "=== 3. Python venv + deps ==="
cd "$APP_DIR"
$PYTHON -m venv venv
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt
pip install -q psutil uvicorn[standard]

echo "=== 4. systemd service ==="
cp "$APP_DIR/deploy/screener.service" /etc/systemd/system/screener.service
systemctl daemon-reload
systemctl enable screener
systemctl restart screener

echo "=== 5. nginx ==="
cp "$APP_DIR/deploy/nginx.conf" /etc/nginx/sites-available/screener
ln -sf /etc/nginx/sites-available/screener /etc/nginx/sites-enabled/screener
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

echo ""
echo "=== Done! ==="
echo "Status: systemctl status screener"
echo "Logs:   journalctl -u screener -f"
