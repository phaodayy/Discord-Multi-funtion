#!/usr/bin/env bash
# Cài Discord-Multi-funtion lên 1 VPS Ubuntu (chạy với quyền root).
# Dùng: curl -fsSL https://raw.githubusercontent.com/phaodayy/Discord-Multi-funtion/main/deploy.sh | bash
set -euo pipefail

REPO="https://github.com/phaodayy/Discord-Multi-funtion.git"
APP_DIR="/opt/discord-multi"
SERVICE="discord-multi"
PORT="${PORT:-8080}"
HOST="${HOST:-0.0.0.0}"

echo ">>> Cài hệ thống phụ thuộc"
apt-get update -y
apt-get install -y git python3 python3-venv python3-pip ufw

echo ">>> Clone / cập nhật code vào $APP_DIR"
if [[ -d "$APP_DIR/.git" ]]; then
  git -C "$APP_DIR" fetch --all
  git -C "$APP_DIR" reset --hard origin/main
else
  git clone "$REPO" "$APP_DIR"
fi

echo ">>> Tạo venv và cài requirements"
python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --upgrade pip
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

ENV_FILE="$APP_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo ">>> Tạo .env mới với mật khẩu ngẫu nhiên"
  PASS="$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)"
  SECRET="$(openssl rand -hex 32)"
  cat > "$ENV_FILE" <<EOF
HOST=$HOST
PORT=$PORT
DASHBOARD_PASSWORD=$PASS
JWT_SECRET=$SECRET
JWT_ALGORITHM=HS256
JWT_EXPIRE_HOURS=72
TOKENS_FILE=tokens.txt
ENABLE_TUNNEL=false
EOF
  chmod 600 "$ENV_FILE"
  NEW_ENV=1
else
  echo ">>> .env đã tồn tại — giữ nguyên"
  NEW_ENV=0
fi

touch "$APP_DIR/tokens.txt"
chmod 600 "$APP_DIR/tokens.txt"

echo ">>> Tạo systemd service: $SERVICE"
cat > "/etc/systemd/system/$SERVICE.service" <<EOF
[Unit]
Description=Discord Multi dashboard
After=network.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/.venv/bin/python -X utf8 $APP_DIR/dashboard.py
Restart=on-failure
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE"
systemctl restart "$SERVICE"

echo ">>> Mở firewall cổng $PORT"
if command -v ufw >/dev/null 2>&1; then
  ufw allow "$PORT"/tcp || true
fi

sleep 2
systemctl --no-pager --full status "$SERVICE" | head -n 15 || true

IP="$(curl -4 -s ifconfig.me || echo 'YOUR_SERVER_IP')"

echo ""
echo "=========================================="
echo " Dashboard: http://$IP:$PORT"
if [[ "$NEW_ENV" == "1" ]]; then
  echo " Mật khẩu đăng nhập: $PASS"
  echo " (Đã ghi vào $ENV_FILE — xem lại bằng: cat $ENV_FILE)"
else
  echo " Mật khẩu lấy từ $ENV_FILE: grep DASHBOARD_PASSWORD $ENV_FILE"
fi
echo ""
echo " Lệnh hữu ích:"
echo "   journalctl -u $SERVICE -f      # xem log"
echo "   systemctl restart $SERVICE     # restart"
echo "   nano $APP_DIR/tokens.txt       # sửa token"
echo "=========================================="
