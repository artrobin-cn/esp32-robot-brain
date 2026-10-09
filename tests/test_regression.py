#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StackChan 桥接服务 · 回归测试
==============================

直接运行（不需要先启动服务）：

    cd pc-bridge
    .venv/bin/python tests/test_regression.py

覆盖范围
--------
A 基础        /health、配置读取
B 鉴权        token 保护、401 / 200
C 视觉判定    真实 OpenCV 路径：黑帧、坏帧、首帧、静态帧累计确认期
D 人脸分支    mock 检测器：有人脸→in_seat，人脸后重置确认计数
E 提醒状态机  阈值 / 复位阈值 / 每日上限 / 非在座抑制
F 活动时段    时段外不提醒
G 全链路      /vision → 计时 → 提醒 → /pending → /ack、/pause
H 安全        白名单拦截、命中放行

说明：不发送真实飞书消息（notify_feishu 被替换为空实现）。
"""

import json
import os
import sys
import time as _time
import types
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

import numpy as np  # noqa: E402

import bridge_server as bs  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=""):
    if cond:
        PASS.append(name)
        print(f"  ✓ {name}")
    else:
        FAIL.append((name, extra))
        print(f"  ✗ {name}   {extra}")


def section(title):
    print(f"\n[{title}]")


# --------------------------------------------------------------------------
# 测试夹具
# --------------------------------------------------------------------------

def jpeg(img_bgr, quality=95):
    import cv2
    ok, buf = cv2.imencode(".jpg", img_bgr, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    assert ok, "JPEG 编码失败"
    return buf.tobytes()


def flat_bright(seed=7, base=120):
    """带固定纹理的静态亮画面（模拟空工位，无脸、画面不动）"""
    rng = np.random.RandomState(seed)
    img = np.full((240, 320, 3), base, dtype=np.int16)
    noise = rng.randint(-12, 13, size=(240, 320, 3))
    return np.clip(img + noise, 0, 255).astype(np.uint8)


def dark_frame():
    return np.zeros((240, 320, 3), dtype=np.uint8)


class FakeYuNet:
    """伪造的人脸检测器：固定返回 N 张脸"""

    def __init__(self, n=1):
        self.n = n

    def setInputSize(self, size):
        self.size = size

    def detect(self, img):
        if self.n <= 0:
            return (None, None)
        row = [100.0, 100.0, 60.0, 60.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.99]
        return (None, np.array([row] * self.n, dtype=np.float32))


def setup_module_state():
    """每个测试组开始前：把全局状态换成干净副本 + 关闭真实副作用"""
    bs.STATE = bs.default_state()
    # 永远处在活动时段（个别测试再单独改）
    bs.CFG["presence"]["active_hours"] = [0, 24]
    bs.CFG["presence"]["active_weekdays"] = [1, 2, 3, 4, 5, 6, 7]
    bs.CFG["presence"]["enabled"] = True
    bs.CFG["presence"]["remind_after_minutes"] = 30
    bs.CFG["presence"]["reset_to_minutes"] = 25
    bs.CFG["presence"]["max_reminders_per_day"] = 6
    # 绝不真的发飞书
    bs.CFG["notify"]["feishu"] = False
    bs.notify_feishu = lambda *a, **k: False
    bs.reset_vision_state()


def seed(state_updates, minutes_in_seat=None):
    """构造状态：可指定已连续在座多少分钟（默认视为当前判定为在座）"""
    st = bs.default_state()
    st["last_presence"] = "in_seat"
    st.update(state_updates)
    if minutes_in_seat is not None:
        st["in_seat_since"] = (datetime.now()
                               - timedelta(minutes=minutes_in_seat)
                               ).isoformat(timespec="seconds")
    bs.STATE = st
    return st


def client():
    bs.app.config["TESTING"] = True
    return bs.app.test_client()


def wait_until(cond, timeout=3.0, interval=0.02):
    """等待异步判定线程落地。

    2026-09-16 起 /vision 的判定与状态机更新在后台线程执行（异步应答），
    POST 返回时状态尚未更新，断言前必须轮询等待，否则出现时序竞态。
    """
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        if cond():
            return True
        _time.sleep(interval)
    return cond()


# --------------------------------------------------------------------------
# A 基础
# --------------------------------------------------------------------------

def test_basics():
    section("A 基础")
    c = client()

    r = c.get("/health")
    check("A1 /health 返回 200", r.status_code == 200, f"got {r.status_code}")
    body = r.get_json() or {}
    check("A2 /health 含 vision_mode", body.get("vision_mode") == "opencv",
          f"got {body.get('vision_mode')}")
    check("A3 /health 含 presence_enabled", "presence_enabled" in body)

    check("A4 配置读取 cfg() 生效",
          bs.cfg("presence", "remind_after_minutes") == 30,
          f"got {bs.cfg('presence', 'remind_after_minutes')}")


# --------------------------------------------------------------------------
# B 鉴权
# --------------------------------------------------------------------------

def test_auth():
    section("B 鉴权")
    c = client()

    bs.CFG["security"]["device_token"] = "s3cret"
    try:
        r = c.get("/pending")
        check("B1 无 token → 401", r.status_code == 401, f"got {r.status_code}")

        r = c.post("/task", json={"command": "整理会议纪要"},
                   headers={"X-Device-Token": "wrong"})
        check("B2 错误 token → 401", r.status_code == 401, f"got {r.status_code}")

        r = c.get("/pending", headers={"X-Device-Token": "s3cret"})
        check("B3 正确 token → 200", r.status_code == 200, f"got {r.status_code}")

        r = c.post("/pause", json={"enabled": True},
                   headers={"X-Device-Token": "s3cret"})
        check("B4 /pause 需要 token（正确时 200）", r.status_code == 200,
              f"got {r.status_code}")

        # token 也支持 query 参数（固件上传 multipart 时方便）
        r = c.get("/pending?token=s3cret")
        check("B5 token 支持 query 参数", r.status_code == 200,
              f"got {r.status_code}")

        r = c.get("/health")
        check("B6 /health 不需要 token（便于探活）", r.status_code == 200,
              f"got {r.status_code}")
    finally:
        bs.CFG["security"]["device_token"] = ""


# --------------------------------------------------------------------------
# C 视觉判定（真实 OpenCV）
# --------------------------------------------------------------------------

def test_vision_real_opencv():
    section("C 视觉判定（真实 OpenCV / YuNet）")

    ok = bs._init_opencv()
    check("C1 OpenCV + 人脸模型可用", ok,
          "YuNet 加载失败，请检查 data/models/ 或 opencv-python 安装")

    # --- 全黑画面：关灯 / 镜头被挡 ---
    bs.reset_vision_state()
    presence, detail = bs.detect_presence_opencv(jpeg(dark_frame()))
    check("C2 全黑画面 → unknown(too_dark)",
          presence == "unknown" and detail.get("reason") == "too_dark",
          f"got {presence}/{detail.get('reason')}")

    # --- 坏数据 ---
    bs.reset_vision_state()
    presence, detail = bs.detect_presence_opencv(b"not-an-image")
    check("C3 无法解码 → unknown(decode_failed)",
          presence == "unknown" and detail.get("reason") == "decode_failed",
          f"got {presence}/{detail.get('reason')}")

    # --- 空工位连续三帧：首帧 unknown，第 2 帧 unknown，第 3 帧 absent ---
    bs.reset_vision_state()
    bs._prev_luma = None        # 明确从零开始
    frame = jpeg(flat_bright())

    p1, d1 = bs.detect_presence_opencv(frame)
    check("C4 静态空工位·首帧 → unknown（无参照帧）",
          p1 == "unknown" and d1.get("reason") == "frame_not_comparable",
          f"got {p1}/{d1.get('reason')}")

    p2, d2 = bs.detect_presence_opencv(frame)
    check("C5 静态空工位·第 2 帧 → unknown（确认期未满）",
          p2 == "unknown", f"got {p2}/{d2.get('reason')}")
    check("C6 第 2 帧 no_motion_streak=1",
          d2.get("no_motion_streak") == 1, f"got {d2.get('no_motion_streak')}")

    p3, d3 = bs.detect_presence_opencv(frame)
    check("C7 静态空工位·第 3 帧 → absent（确认期达标）",
          p3 == "absent" and d3.get("reason") == "no_face_no_motion_confirmed",
          f"got {p3}/{d3.get('reason')}")

    # --- 亮度突变不算"人在动" ---
    bs.reset_vision_state()
    bs.detect_presence_opencv(jpeg(dark_frame() + 0))          # 黑帧建立参照
    p, d = bs.detect_presence_opencv(jpeg(flat_bright()))
    check("C8 黑→亮突变 → unknown（不误判为在座）",
          p == "unknown", f"got {p}/{d.get('reason')}")


# --------------------------------------------------------------------------
# D 人脸分支（mock 检测器，验证分支逻辑）
# --------------------------------------------------------------------------

def test_vision_face_branch():
    section("D 人脸分支（mock 检测器）")

    bs._init_opencv()
    real_yunet = bs._yunet
    try:
        bs.reset_vision_state()
        bs._yunet = FakeYuNet(1)
        p, d = bs.detect_presence_opencv(jpeg(flat_bright()))
        check("D1 检测到人脸 → in_seat", p == "in_seat", f"got {p}")
        check("D2 detail 记录 faces/method",
              d.get("faces") == 1 and d.get("method") == "yunet", f"got {d}")

        # 人脸后 streak 归零：随后第一帧静止应当仍是 unknown（而不是立刻 absent）
        bs._yunet = FakeYuNet(0)
        p, d = bs.detect_presence_opencv(jpeg(flat_bright()))
        check("D3 人脸后第 1 帧无脸静止 → unknown（确认计数已归零）",
              p == "unknown", f"got {p}/{d.get('reason')}")
        p, d = bs.detect_presence_opencv(jpeg(flat_bright()))
        check("D4 紧接着第 2 帧 → absent（确认期达标）",
              p == "absent", f"got {p}/{d.get('reason')}")

        # 无人脸但有明显运动 + 近期在座 → 判为转身/低头
        bs.reset_vision_state()
        bs._yunet = FakeYuNet(0)
        bs._last_in_seat_ts = __import__("time").time()
        f1 = jpeg(flat_bright())
        bs.detect_presence_opencv(f1)                      # 建立参照帧
        moving = flat_bright().copy()
        moving[100:140, 100:180] = 255                     # 局部大变化
        p, d = bs.detect_presence_opencv(jpeg(moving))
        check("D5 无脸+画面在动+近期在座 → in_seat",
              p == "in_seat" and d.get("reason") == "no_face_but_motion_continuous",
              f"got {p}/{d.get('reason')}")

        # 无在座上下文时，运动不代表有人（可能是有人路过）
        bs.reset_vision_state()
        bs._last_in_seat_ts = None
        bs.detect_presence_opencv(f1)
        p, d = bs.detect_presence_opencv(jpeg(moving))
        check("D6 无脸+运动+无在座上下文 → unknown",
              p == "unknown" and d.get("reason") == "motion_without_context",
              f"got {p}/{d.get('reason')}")
    finally:
        bs._yunet = real_yunet
        bs.reset_vision_state()


# --------------------------------------------------------------------------
# E 提醒状态机
# --------------------------------------------------------------------------

def test_reminder_machine():
    section("E 提醒状态机")

    seed({}, minutes_in_seat=10)
    check("E1 在座 10 分钟 → 不提醒", bs.evaluate_reminder(bs.STATE) is None)

    seed({}, minutes_in_seat=29)
    check("E2 在座 29 分钟 → 不提醒", bs.evaluate_reminder(bs.STATE) is None)

    seed({}, minutes_in_seat=30)
    check("E3 在座 30 分钟 → 提醒", bs.evaluate_reminder(bs.STATE) is not None)

    seed({}, minutes_in_seat=60)
    check("E4 在座 60 分钟 → 提醒", bs.evaluate_reminder(bs.STATE) is not None)

    # 非在座一律不提醒
    seed({"last_presence": "absent"}, minutes_in_seat=60)
    check("E5 人不在工位 → 不提醒（别催他站起来）",
          bs.evaluate_reminder(bs.STATE) is None)

    seed({"last_presence": "unknown"}, minutes_in_seat=60)
    check("E6 画面看不清 → 不提醒（宁漏不误）",
          bs.evaluate_reminder(bs.STATE) is None)

    # 提醒后按 reset_to_minutes 重新起算
    st = seed({}, minutes_in_seat=40)
    msg = bs.evaluate_reminder(st)
    bs.push_reminder(st, msg)
    check("E7 提醒后写入 pending",
          len(st["pending"]) == 1 and st["pending"][0]["type"] == "remind_standup")
    check("E8 提醒计数 +1", st["reminders_today"] == 1)

    bs.evaluate_reminder(st)
    check("E9 刚提醒完 5 分钟内不重复提醒",
          bs.evaluate_reminder(st) is None)

    st["last_remind_ts"] = (datetime.now() - timedelta(minutes=24)
                            ).isoformat(timespec="seconds")
    check("E10 提醒后 24 分钟（<25）→ 仍不提醒",
          bs.evaluate_reminder(st) is None)

    st["last_remind_ts"] = (datetime.now() - timedelta(minutes=26)
                            ).isoformat(timespec="seconds")
    check("E11 提醒后 26 分钟（>25）→ 再次提醒",
          bs.evaluate_reminder(st) is not None)

    # 每日上限
    st = seed({"reminders_today": 6}, minutes_in_seat=60)
    check("E12 达到每日上限 6 次 → 不提醒",
          bs.evaluate_reminder(st) is None)

    # 乱数据不应崩
    seed({"last_presence": "in_seat", "in_seat_since": "not-a-timestamp"})
    check("E13 时间戳损坏 → 安全返回 None",
          bs.evaluate_reminder(bs.STATE) is None)


# --------------------------------------------------------------------------
# F 活动时段
# --------------------------------------------------------------------------

def test_active_window():
    section("F 活动时段")

    seed({}, minutes_in_seat=60)
    bs.CFG["presence"]["active_hours"] = [0, 24]
    check("F1 全时段开放 → 提醒",
          bs.evaluate_reminder(bs.STATE) is not None)

    bs.CFG["presence"]["active_hours"] = [23, 24]   # 除 23 点外都不生效
    now_hour = datetime.now().hour
    if now_hour == 23:
        check("F2 当前正处 23 点，跳过时段判定", True)
    else:
        check("F2 不在活动时段 → 不提醒",
              bs.evaluate_reminder(bs.STATE) is None)

    # 周末排除：把今天从 active_weekdays 去掉
    bs.CFG["presence"]["active_hours"] = [0, 24]
    today = datetime.now().weekday() + 1
    bs.CFG["presence"]["active_weekdays"] = [d for d in range(1, 8) if d != today]
    check("F3 今天不在生效星期 → 不提醒",
          bs.evaluate_reminder(bs.STATE) is None)

    bs.CFG["presence"]["active_weekdays"] = [1, 2, 3, 4, 5, 6, 7]

    # 总开关
    seed({}, minutes_in_seat=60)
    bs.CFG["presence"]["enabled"] = False
    check("F4 提醒总开关关闭 → 不提醒",
          bs.evaluate_reminder(bs.STATE) is None)
    bs.CFG["presence"]["enabled"] = True


# --------------------------------------------------------------------------
# G 全链路
# --------------------------------------------------------------------------

def test_end_to_end():
    section("G 全链路 /vision → 提醒 → /pending → /ack")
    c = client()
    real_judge = bs.judge_presence

    try:
        # 1) 照片进来，判定在座 → 开始计时
        bs.STATE = bs.default_state()
        bs.JUDGE = bs.judge_presence
        bs.judge_presence = lambda img, q: ("in_seat", {"mock": True})

        r = c.post("/vision", data={"question": "我在工位吗",
                                    "file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                             "p.jpg")},
                   content_type="multipart/form-data")
        check("G1 /vision 返回 200", r.status_code == 200, f"got {r.status_code}")
        # 异步架构：/vision 立即应答"好的，我看一眼。"，判定结果走后台线程
        check("G2 返回内容为中文人话", "我看一眼" in r.get_data(as_text=True),
              r.get_data(as_text=True)[:60])
        wait_until(lambda: bs.STATE.get("in_seat_since") is not None)
        check("G3 记录到 on_seat 起始时间",
              bs.STATE.get("in_seat_since") is not None)
        check("G4 采样计数 +1", bs.STATE.get("samples_today") == 1)

        # 2) 直接把计时拨到 35 分钟前，再来一张 → 应生成提醒
        bs.STATE["in_seat_since"] = (datetime.now() - timedelta(minutes=35)
                                     ).isoformat(timespec="seconds")
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                         "p.jpg")},
               content_type="multipart/form-data")
        wait_until(lambda: len(bs.STATE.get("pending", [])) == 1)
        check("G5 在座 35 分钟 → 生成提醒",
              len(bs.STATE.get("pending", [])) == 1,
              f"pending={bs.STATE.get('pending')}")

        # 3) 机器人轮询领取
        r = c.get("/pending")
        items = (r.get_json() or {}).get("items", [])
        check("G6 /pending 能领到提醒",
              len(items) == 1 and items[0]["type"] == "remind_standup",
              f"got {items}")

        # 4) 机器人播报完 ack
        r = c.post("/ack", json={"index": 0})
        check("G7 /ack 返回 200", r.status_code == 200)
        check("G8 ack 后 pending 清空",
              len(bs.STATE.get("pending", [])) == 0)

        # 5) 单帧误判不清零计时（本轮修复的核心回归点）
        st = seed({"last_presence": "in_seat"}, minutes_in_seat=35)
        before = st["in_seat_since"]
        bs.judge_presence = lambda img, q: ("absent", {"mock": "yuNet miss"})
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                         "p.jpg")},
               content_type="multipart/form-data")
        wait_until(lambda: bs.STATE.get("absent_streak") == 1)
        check("G9 单帧误判 absent → 不清零在座计时",
              bs.STATE.get("in_seat_since") == before,
              f"{before} -> {bs.STATE.get('in_seat_since')}")
        check("G10 单帧 absent → absent_streak=1",
              bs.STATE.get("absent_streak") == 1,
              f"got {bs.STATE.get('absent_streak')}")

        # 6) 连续第 2 帧 absent → 清零
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                         "p.jpg")},
               content_type="multipart/form-data")
        wait_until(lambda: bs.STATE.get("in_seat_since") is None)
        check("G11 连续 2 帧 absent → 计时归零",
              bs.STATE.get("in_seat_since") is None,
              f"got {bs.STATE.get('in_seat_since')}")

        # 7) /pause 暂停后不再提醒
        bs.judge_presence = lambda img, q: ("in_seat", {"mock": True})
        c.post("/pause", json={"enabled": False})
        st = seed({"last_presence": "in_seat"}, minutes_in_seat=90)
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                         "p.jpg")},
               content_type="multipart/form-data")
        _time.sleep(0.5)  # 等后台判定线程落地，确认它也没有偷偷生成提醒
        check("G12 /pause 暂停后不再生成提醒",
              len(bs.STATE.get("pending", [])) == 0,
              f"pending={bs.STATE.get('pending')}")
        c.post("/pause", json={"enabled": True})

        # 8) /status 可观测
        r = c.get("/status")
        s = r.get_json() or {}
        check("G13 /status 字段完整",
              all(k in s for k in ("presence", "minutes_in_seat", "samples_today",
                                   "pending_count", "in_active_window")),
              f"got {sorted(s.keys())}")
        check("G14 /status 反映暂停状态可恢复",
              s.get("ok") is True)
    finally:
        bs.judge_presence = real_judge


# --------------------------------------------------------------------------
# H 安全
# --------------------------------------------------------------------------

def test_security():
    section("H 安全")
    c = client()

    bs.CFG["security"]["task_keywords"] = ["日志", "报告", "会议", "整理", "提醒"]
    dispatched = []
    real_run = bs.run_hermes_task
    bs.run_hermes_task = lambda cmd, src="stackchan": dispatched.append(cmd)

    try:
        r = c.post("/task", json={"command": "rm -rf / --no-preserve-root"})
        body = r.get_json() or {}
        check("H1 危险命令被白名单拦截",
              body.get("ok") is False and "允许范围" in body.get("error", ""),
              f"got {body}")
        check("H2 被拦截的命令不会真的派发", dispatched == [],
              f"dispatched={dispatched}")

        r = c.post("/task", json={"command": "整理今天的会议纪要"})
        body = r.get_json() or {}
        check("H3 白名单命中的指令放行", body.get("ok") is True, f"got {body}")
        check("H4 放行后确实派发到 Hermes",
              dispatched == ["整理今天的会议纪要"], f"dispatched={dispatched}")

        r = c.post("/task", json={"command": "   "})
        check("H5 空命令 → 400", r.status_code == 400, f"got {r.status_code}")

        r = c.post("/task", json={"text": "整理一下今天的日报"})
        check("H6 兼容 text 字段名",
              (r.get_json() or {}).get("ok") is True, f"got {r.get_json()}")
    finally:
        bs.run_hermes_task = real_run
        bs.CFG["security"]["task_keywords"] = []


# --------------------------------------------------------------------------
# I 反向通道 /notify（P2-⑨）
# --------------------------------------------------------------------------

def test_notify_endpoint():
    section("I 反向通道 /notify")
    c = client()
    bs._notify_seen.clear()

    # 鉴权
    bs.CFG["security"]["device_token"] = "s3cret"
    try:
        r = c.post("/notify", json={"message": "测试"})
        check("I1 /notify 无 token → 401", r.status_code == 401,
              f"got {r.status_code}")
        r = c.post("/notify", json={"message": "主人，飞书有条新消息。"},
                   headers={"X-Device-Token": "s3cret"})
        check("I2 带 token 播报消息入队",
              (r.get_json() or {}).get("ok") is True, f"got {r.get_json()}")
    finally:
        bs.CFG["security"]["device_token"] = ""

    items = bs.STATE.get("pending", [])
    check("I3 入队的是 speak 类型",
          len(items) == 1 and items[0]["type"] == "speak", f"got {items}")

    # 空消息
    r = c.post("/notify", json={"message": "   "})
    check("I4 空消息 → 400", r.status_code == 400, f"got {r.status_code}")

    # 90 秒去重
    r = c.post("/notify", json={"message": "主人，飞书有条新消息。"})
    check("I5 相同内容 90 秒内去重",
          (r.get_json() or {}).get("deduped") is True, f"got {r.get_json()}")
    check("I6 去重不重复入队", len(bs.STATE.get("pending", [])) == 1)

    # alert 类型只上屏
    r = c.post("/notify", json={"type": "alert", "message": "屏幕专属通知"})
    body = r.get_json() or {}
    check("I7 alert 类型入队", body.get("ok") is True and body.get("type") == "alert",
          f"got {body}")
    items = bs.STATE.get("pending", [])
    check("I8 alert 入队类型正确",
          items[-1]["type"] == "alert", f"got {items[-1]}")

    # /pending 能领到、/ack 能清掉
    r = c.get("/pending")
    got = (r.get_json() or {}).get("items", [])
    check("I9 /pending 领到播报与屏显",
          any(i["type"] == "speak" for i in got)
          and any(i["type"] == "alert" for i in got), f"got {got}")
    c.post("/ack", json={})
    check("I10 /ack 后队列清空", len(bs.STATE.get("pending", [])) == 0)


# --------------------------------------------------------------------------
# J 会议模式：/pause 定时自动恢复（P2-⑪）
# --------------------------------------------------------------------------

def test_pause_auto_resume():
    section("J /pause 定时自动恢复")
    c = client()
    try:
        r = c.post("/pause", json={"enabled": False, "minutes": 60})
        body = r.get_json() or {}
        check("J1 暂停返回自动恢复时间",
              body.get("enabled") is False and body.get("auto_resume_at"),
              f"got {body}")
        check("J2 暂停生效", bs.CFG["presence"]["enabled"] is False)

        timer = bs._auto_resume_timer
        check("J3 已安排自动恢复定时器", timer is not None and timer.daemon,
              f"got {timer}")
        # 直接触发定时器回调，模拟 60 分钟后到点
        if timer is not None:
            timer.function()
        check("J4 定时器触发后提醒自动恢复",
              bs.CFG["presence"]["enabled"] is True)

        # 无 minutes 的暂停：不安排自动恢复
        r = c.post("/pause", json={"enabled": False})
        check("J5 无时长暂停 → 不自动恢复",
              (r.get_json() or {}).get("auto_resume_at") is None
              and bs._auto_resume_timer is None, f"got {r.get_json()}")

        # 手动恢复会取消定时器
        c.post("/pause", json={"enabled": False, "minutes": 120})
        r = c.post("/pause", json={"enabled": True})
        check("J6 手动恢复成功且定时器取消",
              bs.CFG["presence"]["enabled"] is True
              and bs._auto_resume_timer is None, f"got {r.get_json()}")
    finally:
        if bs._auto_resume_timer is not None:
            bs._auto_resume_timer.cancel()
            bs._auto_resume_timer = None
        bs.CFG["presence"]["enabled"] = True


# --------------------------------------------------------------------------
# K 白名单 "*" 总开关（P1-⑧）
# --------------------------------------------------------------------------

def test_keyword_wildcard():
    section("K 白名单 * 放行")
    c = client()
    dispatched = []
    real_run = bs.run_hermes_task
    bs.run_hermes_task = lambda cmd, src="stackchan": dispatched.append(cmd)
    try:
        bs.CFG["security"]["task_keywords"] = ["*"]
        r = c.post("/task", json={"command": "随便说点什么都应该放行"})
        check("K1 * 放行一切指令",
              (r.get_json() or {}).get("ok") is True, f"got {r.get_json()}")
        check("K2 * 模式确实派发", len(dispatched) == 1)
        bs.CFG["security"]["task_keywords"] = ["日志"]
        r = c.post("/task", json={"command": "随便说点什么都应该放行"})
        check("K3 普通 白名单仍然拦截",
              (r.get_json() or {}).get("ok") is False, f"got {r.get_json()}")
    finally:
        bs.run_hermes_task = real_run
        bs.CFG["security"]["task_keywords"] = []


# --------------------------------------------------------------------------
# L 日历解析与播报选择（P2-⑨ 电脑侧引擎）
# --------------------------------------------------------------------------

def test_calendar():
    section("L 日历解析与播报选择")
    # 解析：三种格式
    ev = [{"title": "项目评审", "start": "2026-09-11T14:00:00+08:00",
           "location": "3 楼会议室"}]
    a = bs.parse_calendar_events(json.dumps({"ok": True, "data": ev}))
    b = bs.parse_calendar_events(json.dumps(ev))
    c3 = bs.parse_calendar_events("\n".join(json.dumps(e) for e in ev))
    check("L1 解析 {data:[...]} 包裹格式", a == ev)
    check("L2 解析裸数组", b == ev)
    check("L3 解析 NDJSON", c3 == ev)
    check("L4 垃圾输出 → 空列表",
          bs.parse_calendar_events("not json at all") == [])

    norm = bs.normalize_calendar_event(ev[0])
    check("L5 事件归一化出标题/开始/地点",
          norm is not None and norm["title"] == "项目评审"
          and norm["location"] == "3 楼会议室"
          and norm["start"].strftime("%H:%M") == "14:00", f"got {norm}")
    check("L6 缺开始时间的事件无效",
          bs.normalize_calendar_event({"title": "x"}) is None)

    now = datetime(2026, 9, 11, 13, 45, 0)  # 14:00 开场前 15 分钟
    picks = bs.select_announcements(ev, now, [15], set(), [8, 22])
    check("L7 开场前 15 分钟命中播报窗口",
          len(picks) == 1 and "15 分钟后有「项目评审」" in picks[0][1]
          and "3 楼会议室" in picks[0][1], f"got {picks}")

    # 已播报过 → 不再播
    picks2 = bs.select_announcements(ev, now, [15], {picks[0][0]}, [8, 22])
    check("L8 已播报的 key 不重复", picks2 == [])

    # 太早 / 已开场 / 静音时段
    early = bs.select_announcements(ev, datetime(2026, 9, 11, 10, 0), [15],
                                    set(), [8, 22])
    check("L9 离开场还早 → 不播", early == [])
    past = bs.select_announcements(ev, datetime(2026, 9, 11, 14, 1), [15],
                                   set(), [8, 22])
    check("L10 已开场 → 不播", past == [])
    night = bs.select_announcements(ev, datetime(2026, 9, 11, 23, 46), [15],
                                    set(), [8, 22])
    check("L11 深夜不在播报时段 → 不播", night == [])

    # epoch 秒时间戳也能解析
    epoch_ev = [{"summary": "纪要整理", "start_time": 1780000000}]
    check("L12 epoch 秒时间戳可解析",
          bs.normalize_calendar_event(epoch_ev[0]) is not None)


# --------------------------------------------------------------------------
# M 打卡 / 访客 / 下班汇总（P2-⑩ ⑬ ⑭）
# --------------------------------------------------------------------------

def test_punch_visitor_summary():
    section("M 打卡 · 访客 · 下班汇总")
    c = client()
    real_judge = bs.judge_presence
    feishu_sent = []
    real_feishu = bs.notify_feishu
    bs.notify_feishu = lambda text, subject=None: feishu_sent.append(text)
    # notify_feishu 已被换成同步桩，这里临时打开飞书开关以覆盖推送路径
    bs.CFG["notify"]["feishu"] = True

    try:
        bs.CFG["visitor"]["enabled"] = True
        bs.CFG["visitor"]["speak"] = True
        bs.CFG["punch_in"]["enabled"] = True

        # 第一帧：单人 → 打卡，无访客
        bs.judge_presence = lambda img, q: ("in_seat", {"faces": 1})
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                         "p.jpg")},
               content_type="multipart/form-data")
        wait_until(lambda: bs.STATE.get("first_in_seat_ts") is not None)
        check("M1 首次在座记录打卡时间",
              bs.STATE.get("first_in_seat_ts") is not None)
        _time.sleep(0.3)  # 等飞书推送的守护线程落地（桩是同步函数，很快）
        check("M2 打卡推了一条飞书",
              len(feishu_sent) == 1 and "到工位" in feishu_sent[0],
              f"got {feishu_sent}")
        check("M3 单人无访客提醒",
              not any(p["type"] == "speak" for p in bs.STATE.get("pending", [])))

        # 第二帧：两个人入镜 → 访客提醒
        bs.judge_presence = lambda img, q: ("in_seat", {"faces": 2})
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright(3))),
                                         "p.jpg")},
               content_type="multipart/form-data")
        wait_until(lambda: any(p["type"] == "speak"
                               for p in bs.STATE.get("pending", [])))
        speaks = [p for p in bs.STATE.get("pending", []) if p["type"] == "speak"]
        check("M4 两人入镜 → 生成访客播报", len(speaks) == 1, f"got {speaks}")
        _time.sleep(0.3)
        check("M5 访客提醒推飞书", any("工位" in t for t in feishu_sent[1:]),
              f"got {feishu_sent}")
        check("M6 记录访客提醒时间",
              bs.STATE.get("visitor_last_ts") is not None)

        # 冷却期内再来一帧两人 → 不重复提醒
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright(5))),
                                         "p.jpg")},
               content_type="multipart/form-data")
        _time.sleep(0.5)  # 等后台线程落地，确认冷却期没有追加新播报
        speaks = [p for p in bs.STATE.get("pending", []) if p["type"] == "speak"]
        check("M7 冷却期内不重复访客提醒", len(speaks) == 1, f"got {speaks}")

        # 下班汇总文案
        st = bs.default_state()
        st["first_in_seat_ts"] = "2026-09-11T09:05:00"
        st["last_in_seat_ts"] = "2026-09-11T17:40:00"
        st["in_seat_seconds_today"] = 6 * 3600 + 20 * 60
        st["reminders_today"] = 3
        text = bs.compose_daily_summary(st, datetime(2026, 9, 11, 18, 5))
        check("M8 汇总含到岗时间", "09:05" in text, f"got {text}")
        check("M9 汇总含在座时长", "6 小时 20 分钟" in text, f"got {text}")
        check("M10 汇总含提醒次数", "3 次" in text, f"got {text}")

        # 离座后累计秒数入账
        bs.STATE = bs.default_state()
        bs.CFG["presence"]["absent_streak_to_reset"] = 1
        bs.judge_presence = lambda img, q: ("in_seat", {"faces": 1})
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright())),
                                         "p.jpg")},
               content_type="multipart/form-data")
        bs.STATE["in_seat_since"] = (datetime.now() - timedelta(minutes=10)
                                     ).isoformat(timespec="seconds")
        bs.judge_presence = lambda img, q: ("absent", {"faces": 0})
        c.post("/vision", data={"file": (__import__("io").BytesIO(jpeg(flat_bright(9))),
                                         "p.jpg")},
               content_type="multipart/form-data")
        wait_until(lambda: bs.STATE.get("in_seat_seconds_today", 0) >= 9 * 60)
        check("M11 离座后在座段秒数入账",
              bs.STATE.get("in_seat_seconds_today", 0) >= 9 * 60,
              f"got {bs.STATE.get('in_seat_seconds_today')}")
    finally:
        bs.judge_presence = real_judge
        bs.notify_feishu = real_feishu
        bs.CFG["presence"]["absent_streak_to_reset"] = 2


# --------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("StackChan 桥接服务 · 回归测试")
    print("=" * 60)

    tmp = TemporaryDirectory(prefix="stackchan-test-")
    real_state_file = bs.STATE_FILE
    bs.STATE_FILE = Path(tmp.name) / "state.json"
    bs.LOG_FILE = Path(tmp.name) / "bridge.log"

    try:
        setup_module_state()
        test_basics()
        setup_module_state()
        test_auth()
        setup_module_state()
        test_vision_real_opencv()
        setup_module_state()
        test_vision_face_branch()
        setup_module_state()
        test_reminder_machine()
        setup_module_state()
        test_active_window()
        setup_module_state()
        test_end_to_end()
        setup_module_state()
        test_security()
        setup_module_state()
        test_notify_endpoint()
        setup_module_state()
        test_pause_auto_resume()
        setup_module_state()
        test_keyword_wildcard()
        setup_module_state()
        test_calendar()
        setup_module_state()
        test_punch_visitor_summary()
    finally:
        bs.STATE_FILE = real_state_file
        tmp.cleanup()

    total = len(PASS) + len(FAIL)
    print("\n" + "=" * 60)
    print(f"结果：{len(PASS)}/{total} 通过")
    if FAIL:
        print("失败项：")
        for name, extra in FAIL:
            print(f"  ✗ {name}  {extra}")
    else:
        print("全部通过 ✓")
    print("=" * 60)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
