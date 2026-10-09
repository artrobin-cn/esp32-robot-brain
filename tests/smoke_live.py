#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StackChan 桥接服务 · 真实 HTTP 冒烟测试
========================================

和 test_regression.py 的区别：
  - test_regression.py 用 Flask 内置 test_client，**不需要服务在跑**，测逻辑
  - 本脚本用原生 socket 打真实 HTTP 端口，**需要服务已经在跑**，测"真的能被访问"

用途：验证机器人能不能连上（局域网可达性、multipart 解析、端口监听）。

    cd pc-bridge
    ./start.sh start
    .venv/bin/python tests/smoke_live.py            # 默认 127.0.0.1:8787
    .venv/bin/python tests/smoke_live.py <your-mac-ip> 8787   # 测局域网口

退出码 0 = 全通过。
"""

import json
import os
import socket
import sys
from pathlib import Path

import numpy as np

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8787


def _load_token():
    """从 config.yaml 读 device_token（服务端配了 token 就必须带上，否则全 401）"""
    if os.environ.get("SC_DEVICE_TOKEN") is not None:
        return os.environ["SC_DEVICE_TOKEN"]
    try:
        import yaml
        p = Path(__file__).resolve().parent.parent / "config.yaml"
        cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        return (cfg.get("security") or {}).get("device_token") or ""
    except Exception:  # noqa: BLE001
        return ""


TOKEN = _load_token()
AUTH = {"X-Device-Token": TOKEN} if TOKEN else {}

PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append((name, extra))
    print(f"  {'✓' if cond else '✗'} {name}   {extra if not cond else ''}")


def raw(method, path, body=b"", headers=None, ctype=None, timeout=15):
    """发一个最小 HTTP/1.1 请求，返回 (status, text)"""
    s = socket.socket()
    s.settimeout(timeout)
    s.connect((HOST, PORT))
    h = {"Host": f"{HOST}:{PORT}", "Connection": "close"}
    if ctype:
        h["Content-Type"] = ctype
    if headers:
        h.update(headers)
    h["Content-Length"] = str(len(body))
    head = f"{method} {path} HTTP/1.1\r\n" + "".join(
        f"{k}: {v}\r\n" for k, v in h.items()) + "\r\n"
    s.sendall(head.encode("utf-8") + body)

    data = b""
    while True:
        try:
            chunk = s.recv(65536)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    s.close()

    _, _, rest = data.partition(b"\r\n\r\n")
    status_line = data.split(b"\r\n", 1)[0].decode("utf-8", "replace")
    try:
        status = int(status_line.split(" ")[1])
    except (IndexError, ValueError):
        status = -1
    return status, rest.decode("utf-8", "replace")


def jpeg_bytes():
    import cv2
    rng = np.random.RandomState(3)
    base = np.full((240, 320, 3), 120, dtype=np.int16)
    img = np.clip(base + rng.randint(-12, 13, (240, 320, 3)), 0, 255).astype(np.uint8)
    ok, buf = cv2.imencode(".jpg", img, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
    assert ok
    return buf.tobytes()


def multipart(question, image):
    """构造固件同款 multipart 请求体"""
    b = b"--SCBOUND\r\n"
    b += 'Content-Disposition: form-data; name="question"\r\n\r\n'.encode() + question.encode() + b"\r\n"
    b += b"--SCBOUND\r\n"
    b += b'Content-Disposition: form-data; name="file"; filename="photo.jpg"\r\n'
    b += b"Content-Type: image/jpeg\r\n\r\n" + image + b"\r\n"
    b += b"--SCBOUND--\r\n"
    return b


def main():
    print("=" * 58)
    print(f"StackChan 冒烟测试 → http://{HOST}:{PORT}")
    print("=" * 58)

    # 1) 端口可达
    try:
        s = socket.socket()
        s.settimeout(5)
        s.connect((HOST, PORT))
        s.close()
        check(f"S1 {HOST}:{PORT} 端口可达", True)
    except OSError as e:
        check(f"S1 {HOST}:{PORT} 端口可达", False, f"{type(e).__name__}: {e}")
        print("\n服务没在跑，或不在同一网段。先 ./start.sh start")
        return 1

    # 2) /health
    st, body = raw("GET", "/health")
    check("S2 /health 200", st == 200, f"got {st}")
    try:
        h = json.loads(body)
        check("S3 /health 内容正确", h.get("ok") is True,
              f"got {body[:120]}")
        print(f"      vision_mode={h.get('vision_mode')} "
              f"presence_enabled={h.get('presence_enabled')}")
    except json.JSONDecodeError:
        check("S3 /health 返回 JSON", False, body[:120])

    # 3) /status
    st, body = raw("GET", "/status")
    check("S4 /status 200", st == 200, f"got {st}")
    try:
        s = json.loads(body)
        check("S5 /status 字段完整",
              all(k in s for k in ("presence", "minutes_in_seat",
                                   "pending_count", "in_active_window")),
              body[:160])
        print(f"      presence={s.get('presence')} "
              f"minutes_in_seat={s.get('minutes_in_seat')} "
              f"reminders_today={s.get('reminders_today')} "
              f"in_active_window={s.get('in_active_window')}")
    except json.JSONDecodeError:
        check("S5 /status 返回 JSON", False, body[:120])

    # 4) /vision 上传照片（模拟固件）
    st, body = raw(
        "POST", "/vision",
        multipart("我在工位吗", jpeg_bytes()),
        headers={"Device-Id": "SMOKE-TEST", **AUTH},
        ctype="multipart/form-data; boundary=SCBOUND")
    check("S6 /vision 接收 multipart 照片", st == 200, f"got {st} {body[:120]}")
    check("S7 /vision 返回中文判定描述",
          st == 200 and ("工位" in body or "画面" in body),
          body[:120])
    print(f"      判定：{body.strip()[:80]}")

    # 5) /pending
    st, body = raw("GET", "/pending", headers=AUTH)
    check("S8 /pending 200", st == 200, f"got {st}")

    # 6) /task 白名单（注意 Flask jsonify 默认转义非 ASCII，必须解析 JSON 而非字符串匹配）
    st, body = raw("POST", "/task",
                   json.dumps({"command": "整理今天的工作日志"}).encode("utf-8"),
                   headers=AUTH, ctype="application/json")
    try:
        t = json.loads(body)
    except json.JSONDecodeError:
        t = {}
    check("S9 /task 放行白名单指令", st == 200 and t.get("ok") is True,
          f"got {st} {body[:160]}")

    st, body = raw("POST", "/task",
                   json.dumps({"command": "rm -rf / --no-preserve-root"}).encode("utf-8"),
                   headers=AUTH, ctype="application/json")
    try:
        t = json.loads(body)
    except json.JSONDecodeError:
        t = {}
    check("S10 /task 拦截危险命令",
          t.get("ok") is False and "允许范围" in t.get("error", ""),
          f"got {st} {body[:160]}")

    # 7) /pause 开关
    st, body = raw("POST", "/pause", json.dumps({"enabled": True}).encode(),
                   headers=AUTH, ctype="application/json")
    check("S11 /pause 可控", st == 200, f"got {st}")

    # 8) /notify 反向通道（带鉴权 + 去重）
    st, body = raw("POST", "/notify",
                   json.dumps({"message": "冒烟测试：机器人你好"}).encode("utf-8"),
                   headers=AUTH, ctype="application/json")
    try:
        n = json.loads(body)
    except json.JSONDecodeError:
        n = {}
    check("S12 /notify 播报消息入队", st == 200 and n.get("ok") is True,
          f"got {st} {body[:160]}")
    st, body = raw("POST", "/notify",
                   json.dumps({"message": "冒烟测试：机器人你好"}).encode("utf-8"),
                   headers=AUTH, ctype="application/json")
    try:
        n = json.loads(body)
    except json.JSONDecodeError:
        n = {}
    check("S13 /notify 重复消息去重", n.get("deduped") is True,
          f"got {body[:160]}")

    total = len(PASS) + len(FAIL)
    print("\n" + "=" * 58)
    print(f"结果：{len(PASS)}/{total} 通过")
    if FAIL:
        for name, extra in FAIL:
            print(f"  ✗ {name}  {extra}")
    else:
        print("全部通过 ✓ —— 机器人可以连这个地址")
    print("=" * 58)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
