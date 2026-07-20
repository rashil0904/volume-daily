#!/bin/bash
# One-time setup script for DigitalOcean Ubuntu VM.
# Run this once after creating the droplet.
#
# Usage:
#   chmod +x setup_vm.sh
#   ./setup_vm.sh

set -e

REPO_URL="git@github.com:rashil0904/volume-daily.git"
PROJECT_DIR="$HOME/volume-daily"
PYTHON="python3.11"

echo "=== Step 1: System packages + timezone ==="
apt-get update -qq
apt-get install -y python3.11 python3.11-venv python3-pip git
timedatectl set-timezone Asia/Kolkata
echo "  Timezone set to: $(timedatectl | grep 'Time zone')"

echo "=== Step 2: Clone repo ==="
if [ -d "$PROJECT_DIR" ]; then
    echo "  Repo already exists — pulling latest."
    git -C "$PROJECT_DIR" pull
else
    git clone "$REPO_URL" "$PROJECT_DIR"
fi

echo "=== Step 3: Install Python dependencies ==="
cd "$PROJECT_DIR"
$PYTHON -m pip install --quiet -r pipeline/requirements.txt

echo "=== Step 4: Create output directories ==="
mkdir -p data/market_cap_daily data/candles data/instruments results

echo "=== Step 5: .env setup ==="
ENV_FILE="$PROJECT_DIR/pipeline/.env"
if [ -f "$ENV_FILE" ]; then
    echo "  .env already exists — skipping."
else
    echo "  Creating empty .env — fill in your credentials."
    cat > "$ENV_FILE" <<'EOF'
# Zerodha Kite Connect
ZERODHA_API_KEY=
ZERODHA_API_SECRET=
ZERODHA_REDIRECT_URI=https://kite.trade/

# Screener.in
SCREENER_EMAIL=
SCREENER_PASSWORD=

# Upstox data token (long-lived, used by pipeline to fetch candles — analytics only, no trading)
UPSTOX_ACCESS_TOKEN=

# Telegram notifications
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
EOF
    echo "  Edit $ENV_FILE and fill in your credentials before running the pipeline."
fi

echo "=== Step 6: Cron job ==="
CRON_SCRIPT="$PROJECT_DIR/run_pipeline.sh"
CRON_LINE="1 15 * * 1-5 /bin/bash $CRON_SCRIPT >> $HOME/pipeline.log 2>&1"

# Check if cron already set
if crontab -l 2>/dev/null | grep -qF "$CRON_SCRIPT"; then
    echo "  Cron already set — skipping."
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "  Cron set: runs at 3:01 PM IST Mon–Fri."
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Fill in credentials: nano $ENV_FILE"
echo "  2. Test run:            cd $PROJECT_DIR && python3.11 pipeline/run_daily.py"
echo "  3. Check cron:          crontab -l"
echo "  4. View logs:           tail -f $HOME/pipeline.log"
