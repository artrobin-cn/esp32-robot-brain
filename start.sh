#!/bin/bash
# StackChan 桥接服务启动脚本
# 用法：./start.sh [start|stop|restart|status|log|install|uninstall]

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

VENV="$DIR/.venv"
PY="$VENV/bin/python"
PIDFILE="$DIR/data/bridge.pid"
LOGFILE="$DIR/data/bridge.log"
PLIST="$HOME/Library/LaunchAgents/ai.stackchan.bridge.plist"
LABEL="ai.stackchan.bridge"

mkdir -p "$DIR/data"

ensure_venv() {
  if [ ! -x "$PY" ]; then
    echo "→ 首次运行，创建虚拟环境…"
    /usr/bin/python3 -m venv "$VENV"
    "$VENV/bin/pip" install --upgrade pip -q
    echo "→ 安装依赖（含 opencv，约 50MB，耐心等）…"
    "$VENV/bin/pip" install -r requirements.txt -q
    echo "✓ 依赖安装完成"
  fi
}

running_pid() {
  [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null && cat "$PIDFILE"
}

case "${1:-start}" in
  start)
    if pid=$(running_pid); then
      echo "已在运行（PID ${pid}），如需重启请用 ./start.sh restart"
      exit 0
    fi
    # 首次运行：从模板生成本地配置（config.yaml 不入库，避免凭据泄露）
    if [ ! -f config.yaml ] && [ -f config.example.yaml ]; then
      cp config.example.yaml config.yaml
      echo "✓ 已从 config.example.yaml 生成 config.yaml，请先填好 token / API key 再用"
    fi
    ensure_venv
    nohup "$PY" "$DIR/bridge_server.py" >> "$LOGFILE" 2>&1 &
    echo $! > "$PIDFILE"
    sleep 2
    if pid=$(running_pid); then
      HOST_IP=$(ipconfig getifaddr en0 2>/dev/null || echo 127.0.0.1)
      PORT=$(grep -A2 '^server:' config.yaml | grep 'port:' | head -1 | awk '{print $2}')
      echo "✓ 已启动（PID ${pid}）"
      echo "  本机  : http://127.0.0.1:${PORT:-8787}/health"
      echo "  局域网: http://${HOST_IP}:${PORT:-8787}/health   ← 固件里填这个"
    else
      echo "✗ 启动失败，看日志：tail -50 $LOGFILE"
      tail -20 "$LOGFILE" || true
      exit 1
    fi
    ;;
  stop)
    if pid=$(running_pid); then
      kill "$pid" 2>/dev/null || true
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
      rm -f "$PIDFILE"
      echo "✓ 已停止"
    else
      echo "未在运行"
    fi
    ;;
  restart)
    "$0" stop; sleep 1; "$0" start
    ;;
  status)
    if pid=$(running_pid); then
      echo "运行中（PID ${pid}）"
    else
      echo "未运行"
    fi
    curl -s "http://127.0.0.1:8787/status" || echo "（服务无响应）"
    echo
    ;;
  log)
    tail -f "$LOGFILE"
    ;;
  install)
    ensure_venv
    cat > "$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY}</string>
    <string>${DIR}/bridge_server.py</string>
  </array>
  <key>WorkingDirectory</key><string>${DIR}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>${LOGFILE}</string>
  <key>StandardErrorPath</key><string>${LOGFILE}</string>
</dict>
</plist>
PLIST_EOF
    "$0" stop || true
    launchctl unload "$PLIST" 2>/dev/null || true
    launchctl load "$PLIST"
    echo "✓ 已装成开机自启服务（${LABEL}）"
    echo "  卸载：./start.sh uninstall"
    ;;
  uninstall)
    "$0" stop || true
    launchctl unload "$PLIST" 2>/dev/null || true
    rm -f "$PLIST"
    echo "✓ 已卸载开机自启"
    ;;
  *)
    echo "用法: $0 [start|stop|restart|status|log|install|uninstall]"
    exit 1
    ;;
esac
