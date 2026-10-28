#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StackChan 桥接服务
------------------------------------
职责：
  1. /vision  接收机器人定时上传的照片，判断"人在不在工位"，维护久坐计时
  2. /task    接收自然语言任务，转交本机 Hermes Agent 执行
  3. /pending 机器人轮询领取"该提醒了/该播报了"的指令
  4. /status  查看当前状态与统计
  5. /notify  反向通道：飞书/日历/外部程序推一句话，机器人开口播报

设计原则：机器人只负责耳眼手，长内容一律推飞书，不朗读。
（例外：提醒类短句会经 /tts/synth 合成语音后用机器人本地喇叭播放——
 2026-09-14 实测小智云端不认 WakeWordInvoke 注入的文字（只当误唤醒处理，
 进聆听态等 65s 超时），云端 TTS 播报路线废弃，改为桥接合成 + 机器人本地播。）

启动：python3 bridge_server.py
健康检查：curl http://127.0.0.1:8787/health
"""

import asyncio
import hashlib
import io
import json
import os
import random
import re
import socket
import struct
import subprocess
import sys
import os
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yaml
except ImportError:
    print("缺少依赖：pyyaml。请先执行 pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

try:
    from flask import Flask, request, jsonify, send_file
except ImportError:
    print("缺少依赖：flask。请先执行 pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
if not DATA_DIR.exists():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
PHOTO_DIR = DATA_DIR / "photos"
STATE_FILE = DATA_DIR / "state.json"
LOG_FILE = DATA_DIR / "bridge.log"
TTS_DIR = DATA_DIR / "tts"
# Dashboard（2026-09-16）：视觉分析流水（每行一个 JSON：拍照判定 / 喝水判定）
ANALYSIS_FILE = DATA_DIR / "analysis.jsonl"
# Dashboard 设置（视觉 API 厂商/模型/key，在页面上自己填，独立文件避免覆盖 config.yaml 注释）
API_SETTINGS_FILE = DATA_DIR / "api_settings.json"
# 设备侧可热更新配置（2026-09-16）：拍摄位等，设备经 /device_config 轮询拉取，
# 免烧录改舵机角度（产品化不能让用户烧录调角度）
DEVICE_SETTINGS_FILE = DATA_DIR / "device_settings.json"
# 固件内置默认值（与 hal_hermes.cpp 的编译期常量保持一致）
_PHOTO_POS_DEFAULTS = {
    "seat": {"yaw": 20, "pitch": 30},
    "cup":  {"yaw": -60, "pitch": 10},
}
# 空闲休眠热配置（2026-09-16 产品决策产品化）：enabled=False 陪伴模式下永不休眠；
# idle_sleep_minutes 为空闲多少分钟后休眠（固件默认 5 分钟）
_POWER_SAVE_DEFAULTS = {"enabled": True, "idle_sleep_minutes": 5}

app = Flask(__name__)

# ---- Dashboard 运行时跟踪（只存内存，不落盘）----
_STARTED_AT = time.time()                       # bridge 进程启动时刻
_DEVICE_LAST_SEEN = {"ts": 0.0}                 # 设备最近一次来轮询/拍照的时间
_FEISHU_LAST = {"ts": None, "ok": None, "error": ""}   # 飞书最近一次推送结果
_ANALYSIS_LOCK = threading.Lock()

# --------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "server": {"host": "0.0.0.0", "port": 8787},
    "hermes": {
        "bin": "hermes",
        "extra_args": [],
        "feishu_target": "",
        "task_timeout": 600,
    },
    "vision": {"mode": "opencv", "yunet_model": "", "ollama_url": "",
               "ollama_model": "", "dark_threshold": 25},
    "presence": {
        "enabled": True, "sample_minutes": 5, "remind_after_minutes": 30,
        "reset_to_minutes": 25, "absent_streak_to_reset": 2,
        "max_reminders_per_day": 6, "active_hours": [9, 18],
        "active_weekdays": [1, 2, 3, 4, 5],
        "messages": ["该起身活动一下了。"],
    },
    "notify": {"feishu": True, "save_photos": False, "photo_retention_days": 3},
    "security": {"task_keywords": [], "device_token": ""},
    # 反向通道：日历到点 → 机器人开口播报（P2-⑨，2026-09-11 产品决策要做）
    "calendar": {
        "enabled": False,
        # 输出 JSON 的命令。支持占位符 {start} {end}（ISO8601，今天/明天 0 点，含时区）。
        # 推荐：lark-cli calendar +agenda --start "{start}" --end "{end}" --format json
        # 输出兼容 {"ok":true,"data":[...]}、裸数组、NDJSON（每行一个对象）。
        "fetch_cmd": "",
        "poll_seconds": 60,
        "lead_minutes": [15],       # 开场前多久播报，可多个如 [30, 5]
        "announce_hours": [8, 22],  # 只在这个钟点区间播报
        "max_speaks_per_day": 12,
        "feishu": True,             # 播报同时给飞书留痕
    },
    # 访客提醒（P2-⑬）：镜头里同时出现多张脸 → 推飞书 + 机器人说一声
    "visitor": {"enabled": True, "min_faces": 2, "cooldown_minutes": 10,
                "speak": True},
    # 打卡式记录（P2-⑭）：每天第一次在座推飞书
    "punch_in": {"enabled": True},
    # 下班前自动汇总（P2-⑩）
    "daily_summary": {"enabled": True, "hour": 18, "minute": 5,
                      "weekdays": [1, 2, 3, 4, 5]},
    # 专注度（P2-⑫）：mode=ollama 时可让模型顺带判断"是否在玩手机"
    "focus": {"enabled": False, "cooldown_minutes": 45},
}


def load_config():
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    path = BASE_DIR / "config.yaml"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            user_cfg = yaml.safe_load(f) or {}
        for section, values in user_cfg.items():
            if isinstance(values, dict) and isinstance(cfg.get(section), dict):
                cfg[section].update(values)
            else:
                cfg[section] = values
    return cfg


CFG = load_config()
_config_lock = threading.Lock()


def cfg(*keys, default=None):
    cur = CFG
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# --------------------------------------------------------------------------
# 日志
# --------------------------------------------------------------------------

_log_lock = threading.Lock()
_IS_TTY = sys.stdout is not None and sys.stdout.isatty()


def log(msg, level="INFO"):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}"
    # 前台运行时打到终端；后台运行时只写文件，避免与 nohup 重定向重复
    if _IS_TTY:
        print(line, flush=True)
    with _log_lock:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


# --------------------------------------------------------------------------
# 状态
# --------------------------------------------------------------------------

def _today():
    return datetime.now().strftime("%Y-%m-%d")


def default_state():
    return {
        "date": _today(),
        "in_seat_since": None,       # 本轮连续在座起始时间戳
        "absent_streak": 0,          # 连续"无人"次数
        "last_vision_ts": None,
        "last_presence": "unknown",  # in_seat / absent / unknown
        "last_remind_ts": None,
        "reminders_today": 0,
        "pending": [],               # 待机器人领取的提醒
        "samples_today": 0,
        "last_photo_meta": {},
        # ---- 以下为 2026-09-11 扩展（打卡/汇总/日历播报/访客）----
        "first_in_seat_ts": None,    # 今天第一次确认在座（打卡用）
        "last_in_seat_ts": None,     # 今天最后一次确认在座（下班汇总用）
        "in_seat_seconds_today": 0,  # 已结束的在座段累计秒数（不含当前段）
        "announced_events": [],      # 今天已播报过的日程 key，防重复
        "speaks_today": 0,           # 今天日历/外部播报计数
        "visitor_last_ts": None,     # 上次访客提醒时间（冷却用）
        "focus_last_ts": None,       # 上次"不专注"提醒时间（冷却用）
        "summary_sent_date": None,   # 下班汇总今天发过没有
        # ---- 以下为 2026-09-16 健康陪伴扩展（喝水提醒）----
        "water_level": None,         # 最近一次水位判定：full / low / empty / None(未判定)
        "water_last_ts": None,       # 最近一次水位判定时间
        "water_last_remind_ts": None,  # 上次喝水提醒时间（冷却用）
        "water_reminds_today": 0,    # 今日喝水提醒次数
        # ---- 以下为 2026-09-16 行为分析扩展（产品立项：坐姿 + 水位线追踪）----
        "posture_last": None,          # 最近一次坐姿判定：good / hunching / None
        "posture_consecutive_hunch": 0,  # 连续驼背/趴桌轮数（每轮约 5 分钟）
        "posture_last_remind_ts": None,  # 上次坐姿提醒时间（冷却用）
        "posture_reminds_today": 0,      # 今日坐姿提醒次数
        "water_series": [],            # [(ts, percent)] 水位时间序列，保留最近 3 小时
        "water_last_change_ts": None,  # 最近一次水位"显著变化"（Δ≥10%）时间
        "water_still_last_remind_ts": None,  # 上次"水位不动"提醒时间（冷却用）
        "water_still_reminds_today": 0,      # 今日"水位不动"提醒次数
    }


_state_lock = threading.Lock()


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
            base = default_state()
            base.update(st)
            return base
        except (json.JSONDecodeError, OSError):
            log("state.json 读取失败，使用初始状态", "WARN")
    return default_state()


def save_state(st):
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    tmp.replace(STATE_FILE)


STATE = load_state()


def reset_daily_if_needed(st):
    if st.get("date") != _today():
        st["date"] = _today()
        st["in_seat_since"] = None
        st["absent_streak"] = 0
        st["reminders_today"] = 0
        st["samples_today"] = 0
        st["last_remind_ts"] = None
        st["first_in_seat_ts"] = None
        st["last_in_seat_ts"] = None
        st["in_seat_seconds_today"] = 0
        st["announced_events"] = []
        st["speaks_today"] = 0
        st["visitor_last_ts"] = None
        st["focus_last_ts"] = None
        st["water_reminds_today"] = 0
        st["posture_reminds_today"] = 0
        st["water_still_reminds_today"] = 0
        st["posture_consecutive_hunch"] = 0
        log("跨天，已重置每日统计")


# --------------------------------------------------------------------------
# 图像判定
# --------------------------------------------------------------------------

_yunet = None
_cascade = None
_sface = None              # SFace 人脸识别模型（认"主人"而非"任意脸"）
_user_templates = None     # 主人特征模板 np.ndarray (N, 128)，注册后常驻
_cv2_ready = None
_prev_gray = None
_prev_luma = None
_prev_presence = None
_no_motion_streak = 0       # 连续"无人脸 + 画面不动"的采样次数
_last_in_seat_ts = None     # 最近一次确认在座的时间（用于"转身/低头"判据）

YUNET_URLS = [
    "https://github.com/opencv/opencv_zoo/raw/main/models/"
    "face_detection_yunet/face_detection_yunet_2023mar.onnx",
    "https://raw.githubusercontent.com/opencv/opencv_zoo/main/models/"
    "face_detection_yunet/face_detection_yunet_2023mar.onnx",
]
MODEL_DIR = DATA_DIR / "models"
YUNET_FILE = MODEL_DIR / "face_detection_yunet_2023mar.onnx"
SFACE_FILE = MODEL_DIR / "face_recognition_sface_2021dec.onnx"
USER_FACE_FILE = DATA_DIR / "user_face.npz"   # 主人特征模板（可多张）
OWNER_REF_FILE = DATA_DIR / "owner_ref.jpg"   # Dashboard 上传的参考照（仅展示）


def _download_yunet():
    """首次运行时自动拉取 YuNet 模型（约 340KB）"""
    import urllib.request

    if not MODEL_DIR.exists():
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
    for url in YUNET_URLS:
        try:
            log(f"正在下载人脸检测模型（约 340KB）：{url}")
            tmp = YUNET_FILE.with_suffix(".part")
            urllib.request.urlretrieve(url, tmp)
            tmp.replace(YUNET_FILE)
            log(f"模型已保存到 {YUNET_FILE}")
            return True
        except Exception as e:  # noqa: BLE001
            log(f"下载失败（换下一个源）：{type(e).__name__}: {e}", "WARN")
    log("模型自动下载失败。请手动下载 face_detection_yunet_2023mar.onnx 放到 "
        f"{MODEL_DIR} ，或把 config.yaml 里 vision.yunet_model 指向已有文件。", "ERROR")
    return False


def _load_user_templates():
    """加载主人特征模板（data/user_face.npz，键 features = (N,128)）。

    2026-10-10 人脸识别上线：YuNet 只能说"有张脸"，SFace 特征比对才能
    认出"这是主人"。没有模板时识别关闭，行为退回纯检测模式。
    """
    global _user_templates
    if not USER_FACE_FILE.exists():
        _user_templates = None
        return None
    try:
        import numpy as np
        data = np.load(USER_FACE_FILE)
        feats = np.asarray(data["features"], dtype=np.float32)
        if feats.ndim == 1:
            feats = feats.reshape(1, -1)
        if feats.size == 0:
            _user_templates = None
            return None
        _user_templates = feats
        return feats.shape[0]
    except Exception as e:  # noqa: BLE001
        log(f"主人模板加载失败：{e}", "WARN")
        _user_templates = None
        return None


def _init_opencv():
    """惰性加载 OpenCV。返回 True 表示人脸检测可用。"""
    global _cv2_ready, _yunet, _cascade, _sface
    if _cv2_ready is not None:
        return _cv2_ready
    try:
        import cv2
    except ImportError:
        log("未安装 opencv-python，视觉判定不可用。"
            "安装：pip install opencv-python-headless numpy", "ERROR")
        _cv2_ready = False
        return False

    log(f"OpenCV 版本：{cv2.__version__}")

    # ---- 首选：YuNet（OpenCV 4.5.4+ / 5.x 均支持，准确率高）----
    model_path = (cfg("vision", "yunet_model", default="") or "").strip()
    candidate = Path(model_path) if model_path else YUNET_FILE
    if not candidate.exists() and not model_path:
        _download_yunet()
    if candidate.exists():
        try:
            _yunet = cv2.FaceDetectorYN.create(str(candidate), "", (320, 240),
                                               score_threshold=0.5)
            log(f"已加载 YuNet 人脸检测模型：{candidate}")
        except Exception as e:  # noqa: BLE001
            log(f"YuNet 加载失败：{e}", "WARN")
            _yunet = None

    # ---- 兜底：Haar（仅 OpenCV 4.x 有；5.0 已移除，会走不到这里）----
    if _yunet is None and hasattr(cv2, "CascadeClassifier"):
        try:
            xml = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
            _cascade = cv2.CascadeClassifier(xml)
            if _cascade.empty():
                raise RuntimeError("Haar 分类器加载为空")
            log("YuNet 不可用，回退 Haar（准确率较低，建议装好 YuNet）", "WARN")
        except Exception as e:  # noqa: BLE001
            log(f"Haar 加载失败：{e}", "WARN")
            _cascade = None

    if _yunet is None and _cascade is None:
        log("没有人脸检测模型，判定将始终为 unknown（不会误提醒，但久坐提醒也"
            "不会生效）。请确认模型文件已就位。", "ERROR")

    # ---- 人脸识别（SFace）：config vision.recog_enabled 开启 ----
    if _yunet is not None and cfg("vision", "recog_enabled", default=False):
        sface_path = (cfg("vision", "recog_model", default="") or "").strip()
        sface_candidate = Path(sface_path) if sface_path else SFACE_FILE
        if sface_candidate.exists():
            try:
                _sface = cv2.FaceRecognizerSF.create(str(sface_candidate), "")
                n = _load_user_templates()
                if n:
                    log(f"已加载 SFace 人脸识别模型 + 主人模板 {n} 张："
                        f"{sface_candidate}")
                else:
                    log("已加载 SFace 模型，但尚无主人模板（POST /enroll 注册），"
                        "识别暂不生效", "WARN")
                    _sface = None
            except Exception as e:  # noqa: BLE001
                log(f"SFace 加载失败：{e}", "WARN")
                _sface = None
        else:
            log(f"SFace 模型不存在（{sface_candidate}），人脸识别未启用", "WARN")

    _cv2_ready = True
    return _yunet is not None or _cascade is not None


def _seat_roi_rect(shape_wh=None):
    """读取 vision.seat_roi（归一化 [x, y, w, h]）。返回 (rx, ry, rw, rh) 或 None。

    2026-10-09 新增：摄像头广角视野里会出现邻座/他人，导致"人不在座却判在座"。
    seat_roi 把人脸判定和帧差都限定在用户自己的座位区域内，区域外的人脸忽略。
    """
    roi = cfg("vision", "seat_roi", default=None)
    if not roi:
        return None
    try:
        rx, ry, rw, rh = (float(v) for v in roi)
    except (TypeError, ValueError):
        return None
    if not (0 <= rx < 1 and 0 <= ry < 1 and 0 < rw <= 1 and 0 < rh <= 1
            and rx + rw <= 1 and ry + rh <= 1):
        return None
    return rx, ry, rw, rh


def _count_faces_in_roi(dets, roi, frame_w, frame_h):
    """统计落在 seat_roi 内的人脸。以人脸框中心点是否在区域内为准。

    返回 (inside, outside, large_inside)。roi 为 None 时全部计入 inside。
    large_inside 是框宽 >= visitor.min_face_px 的人脸数（访客判定用——
    远处邻座只有小脸，贴近工位的真访客才会形成大脸）。
    """
    min_px = float(cfg("visitor", "min_face_px", default=80))
    if roi is None:
        inside = int(len(dets))
        large = sum(1 for d in dets
                    if len(d) >= 4 and float(d[2]) >= min_px)
        return inside, 0, large
    rx, ry, rw, rh = roi
    inside = 0
    outside = 0
    large = 0
    for d in dets:
        try:
            x, y, fw, fh = float(d[0]), float(d[1]), float(d[2]), float(d[3])
        except (TypeError, ValueError, IndexError):
            continue
        cx = (x + fw / 2) / max(frame_w, 1)
        cy = (y + fh / 2) / max(frame_h, 1)
        if rx <= cx <= rx + rw and ry <= cy <= ry + rh:
            inside += 1
            if fw >= min_px:
                large += 1
        else:
            outside += 1
    return inside, outside, large


def _cosine_sim(a, b):
    import numpy as np
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-9))


def _recognize_owner(img, dets, roi, frame_w, frame_h):
    """对 ROI 内的每张脸提取 SFace 特征，与主人模板比对。

    返回 dict：
      user_sims     主人匹配到的最大相似度（无人脸/未启用时 None）
      user_faces    判定为主人的人脸数（ROI 内）
      stranger_faces  ROI 内"不是主人"的大脸数（访客判定用）
      sims          ROI 内每张脸的 (best_sim, face_w) 列表（调试用）
    框过小（< recog_min_face_px，默认 60px）的特征不可靠，跳过识别
    （远处小脸不算主人也不算陌生人）。
    """
    out = {"user_sims": None, "user_faces": 0, "stranger_faces": 0, "sims": [],
           "roi_faces": 0}
    if _sface is None or _user_templates is None or dets is None:
        return out
    import numpy as np

    min_px = float(cfg("vision", "recog_min_face_px", default=60))
    thr = float(cfg("vision", "recog_threshold", default=0.363))
    # 2026-10-27 用户反馈「我走了以后，它把右后方的女生认成我」：0.363 是
    # OpenCV 官方推荐值，本来就偏松，擦线误报压不住。判定「是主人」改用
    # 更高的门槛（实测主人 98.8% 仍通过、历史陌生人全被挡）；0.363~0.42
    # 之间算灰区——既不认作主人也不报访客，避免两头误判。
    owner_thr = float(cfg("vision", "recog_owner_threshold", default=0.42))
    rx = ry = rw = rh = None
    if roi is not None:
        rx, ry, rw, rh = roi

    best_user = None
    for d in dets:
        try:
            x, y, fw, fh = float(d[0]), float(d[1]), float(d[2]), float(d[3])
        except (TypeError, ValueError, IndexError):
            continue
        cx = (x + fw / 2) / max(frame_w, 1)
        cy = (y + fh / 2) / max(frame_h, 1)
        if roi is not None and not (rx <= cx <= rx + rw
                                    and ry <= cy <= ry + rh):
            continue
        # ROI 内有脸（不论大小/认不认得出）：给"有人就算在座"规则用。
        # 人离远了脸只有 40~55px，识别不可靠，但"这里坐着人"是确定的。
        out["roi_faces"] += 1
        if fw < min_px:
            continue
        try:
            aligned = _sface.alignCrop(img, d)
            feat = np.asarray(_sface.feature(aligned), dtype=np.float32)\
                .reshape(-1)
        except Exception:  # noqa: BLE001
            continue
        sims = [_cosine_sim(feat, t) for t in _user_templates]
        best = max(sims) if sims else -1.0
        out["sims"].append((round(best, 3), int(fw)))
        if best >= owner_thr:
            out["user_faces"] += 1
            if best_user is None or best > best_user:
                best_user = best
        elif best < thr and fw >= float(cfg("visitor", "min_face_px",
                                           default=80)):
            out["stranger_faces"] += 1
        # 灰区（thr <= best < owner_thr）：不认主人也不报访客，留档待定标
        if thr - 0.08 <= best <= owner_thr + 0.13:
            try:
                with open(DATA_DIR / "recog_gray.log", "a",
                          encoding="utf-8") as gf:
                    gf.write("%s sim=%.3f w=%d roi=%s\n"
                             % (datetime.now().isoformat(timespec="seconds"),
                                best, int(fw),
                                "y" if roi is not None else "n"))
            except OSError:
                pass
    if best_user is not None:
        out["user_sims"] = round(best_user, 3)
    return out


def _enroll_owner_face(image_bytes, replace=False):
    """从照片里提取最大人脸特征，注册/追加为主人模板。

    返回 (ok, info)。照片里没检测到脸时报错。
    """
    if not _init_opencv() or _yunet is None:
        return False, "人脸检测模型不可用"
    if _sface is None:
        return False, "SFace 模型不可用（检查 vision.recog_enabled 与模型文件）"
    import cv2
    import numpy as np

    arr = np.frombuffer(image_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return False, "照片解码失败"
    h, w = img.shape[:2]
    _yunet.setInputSize((w, h))
    result = _yunet.detect(img)
    dets = result[1] if isinstance(result, tuple) and len(result) > 1 else result
    if dets is None or not len(dets):
        return False, "照片里没有检测到人脸"
    # 取最大脸（注册照默认就是主人单人照）
    d = max(dets, key=lambda r: float(r[2]) * float(r[3]))
    if float(d[2]) < 60:
        return False, f"人脸太小（{float(d[2]):.0f}px），请靠近摄像头重拍"
    aligned = _sface.alignCrop(img, d)
    feat = np.asarray(_sface.feature(aligned), dtype=np.float32).reshape(-1)

    # 与已有模板的最高相似度（自查一致性：低说明两次注册的可能不是同一人）
    self_sim = None
    if _user_templates is not None and not replace:
        self_sim = round(max(_cosine_sim(feat, t)
                             for t in _user_templates), 3)
        feats = np.vstack([_user_templates, feat.reshape(1, -1)])
    else:
        feats = feat.reshape(1, -1)
    np.savez(USER_FACE_FILE, features=feats)
    _load_user_templates()
    return True, {"templates": int(feats.shape[0]), "self_sim": self_sim}


def detect_presence_opencv(image_bytes):
    """
    返回 (presence, detail)
      presence: in_seat / absent / unknown

    判据优先级（配置 vision.seat_roi 后，人脸与帧差均只看座位区域）：
      1. 画面过暗                     -> unknown（关灯/遮挡，不能判断）
      2. 座位区域内检测到人脸          -> in_seat（最可靠）
      3. 亮度突变 / 首帧 / 算不出帧差   -> unknown（不改变计时）
      4. 区域内有变化 + 近期在座        -> in_seat（转身/低头/侧脸）
      5. 区域内几乎不动，连续 N 次      -> absent（人不在工位）
      6. 其余                          -> unknown
    """
    global _prev_gray, _prev_luma, _no_motion_streak, _last_in_seat_ts

    if not _init_opencv():
        return "unknown", {"reason": "no_face_detector"}

    import cv2
    import numpy as np

    arr = np.frombuffer(image_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return "unknown", {"reason": "decode_failed"}

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    mean_luma = float(gray.mean())
    dark_threshold = float(cfg("vision", "dark_threshold", default=25))

    # --- 帧差（画面变化量）---
    # seat_roi 生效时帧差只算座位区域，邻座/背景里的活动不再污染判定
    roi = _seat_roi_rect()
    motion = None
    try:
        small = cv2.resize(gray, (80, 60)).astype(np.int16)
        if roi is not None:
            rx, ry, rw, rh = roi
            sx0, sy0 = int(rx * 80), int(ry * 60)
            sx1, sy1 = int(round((rx + rw) * 80)), int(round((ry + rh) * 60))
            if sx1 - sx0 >= 8 and sy1 - sy0 >= 8:
                small = small[sy0:sy1, sx0:sx1]
            else:
                roi = None  # 区域过小，退回全帧
        if _prev_gray is not None and _prev_gray.shape == small.shape:
            motion = float(np.abs(small - _prev_gray).mean())
        _prev_gray = small
    except Exception:  # noqa: BLE001
        motion = None

    # --- 整体亮度突变检测：从黑到亮这类变化不是"人在动" ---
    luma_delta = None
    if _prev_luma is not None:
        luma_delta = abs(mean_luma - _prev_luma)
    _prev_luma = mean_luma

    if mean_luma < dark_threshold:
        return "unknown", {"reason": "too_dark", "mean_luma": round(mean_luma, 1),
                           "motion": motion}

    faces = 0
    faces_out_roi = 0
    faces_large = 0
    method = "none"
    dets_in = []
    if _yunet is not None:
        h, w = img.shape[:2]
        _yunet.setInputSize((w, h))
        result = _yunet.detect(img)
        dets = result[1] if isinstance(result, tuple) and len(result) > 1 else result
        if dets is not None and len(dets):
            faces, faces_out_roi, faces_large = _count_faces_in_roi(
                dets, roi, w, h)
            dets_in = list(dets)
        method = "yunet"
    elif _cascade is not None:
        rects = _cascade.detectMultiScale(gray, scaleFactor=1.1,
                                          minNeighbors=5, minSize=(40, 40))
        if rects is not None and len(rects):
            faces, faces_out_roi, faces_large = _count_faces_in_roi(
                rects, roi, img.shape[1], img.shape[0])
        method = "haar"

    # --- 人脸识别（SFace）：ROI 内的脸是主人还是别人 ---
    recog = _recognize_owner(img, dets_in, roi, w, h) \
        if (dets_in and _yunet is not None and roi is not None) \
        else {"user_sims": None, "user_faces": 0, "stranger_faces": 0,
              "sims": [], "roi_faces": 0}
    user_faces = recog["user_faces"]

    detail = {"method": method, "faces": faces, "motion": motion,
              "faces_large": faces_large,
              "user_faces": user_faces,
              "user_sim": recog["user_sims"],
              "stranger_faces": recog["stranger_faces"],
              "roi_faces": recog.get("roi_faces", 0),
              "faces_out_roi": faces_out_roi if roi is not None else None,
              "luma_delta": None if luma_delta is None else round(luma_delta, 1),
              "no_motion_streak": _no_motion_streak,
              "mean_luma": round(mean_luma, 1)}

    now = time.time()

    # ① 座位区域内认出主人 —— 最可靠的信号（未配 ROI 时退回"有脸即在座"）
    if user_faces >= 1 or (faces >= 1 and roi is None):
        _no_motion_streak = 0
        _last_in_seat_ts = now
        return "in_seat", {**detail,
                           "reason": "owner_face" if user_faces >= 1
                           else "face"}

    # ② 座位区域里有脸但都没认出是谁（2026-10-15 用户两轮反馈定稿）：
    #    仅当"没有任何大脸被判为陌生人"时才算在座——这种情况下 ROI 里的脸
    #    要么太小（<60px，识别被跳过，只可能是主人坐远了），要么特征提取
    #    失败。若有大脸认不出（可能是陌生人占座）则落到 ②b 拒绝在座。
    if (cfg("vision", "in_seat_any_face", default=True)
            and recog.get("roi_faces", 0) >= 1
            and recog["stranger_faces"] == 0):
        _no_motion_streak = 0
        _last_in_seat_ts = now
        return "in_seat", {**detail, "reason": "face_unrecognized"}

    # ②b 座位区域有大脸但不是主人 —— 拒绝在座（可能是陌生人占座），
    # 访客提醒逻辑会单独处理 stranger_faces
    if faces >= 1 and recog["stranger_faces"] >= 1:
        return "unknown", {**detail, "reason": "face_not_owner"}

    # 亮度突变（开关灯、屏幕大面积切换）时帧差不可信；首帧也没有参照
    luma_guard = float(cfg("vision", "luma_change_guard", default=20.0))
    lighting_changed = luma_delta is not None and luma_delta >= luma_guard
    if motion is None or lighting_changed:
        return "unknown", {**detail, "reason": "frame_not_comparable"}

    motion_in_seat = float(cfg("vision", "motion_in_seat_threshold", default=2.5))
    # 无人脸时的动作兜底门槛要比有常人时的噪音高：办公室里灯光/远处同事/
    # 窗帘等会在 ROI 内制造 2.5~4.3 的持续低幅噪音，曾把 27 分钟的离座
    # 全程刷成"在座"（2026-10-09 实测）。有人脸确认走①，这里只兜"转身/
    # 低头但身体仍在活动"的情况。
    no_face_motion = float(cfg("vision", "no_face_motion_threshold",
                               default=4.5))
    # 动作量过大的"无人脸"帧更像人在走动/出入画面/光线剧变，而不是坐在
    # 工位上工作（起身离开瞬间实测 50~70），同样不认。
    no_face_motion_cap = float(cfg("vision", "no_face_motion_cap",
                                   default=12.0))

    # ② 画面有变化 —— 人可能在动
    if motion >= motion_in_seat:
        _no_motion_streak = 0
        # 最近一段时间内确认过在座，才认为是同一个人转身/低头（而不是有人路过）
        window_min = float(cfg("vision", "in_seat_context_minutes", default=30))
        if (_last_in_seat_ts is not None
                and (now - _last_in_seat_ts) <= window_min * 60):
            if no_face_motion <= motion < no_face_motion_cap:
                # 注意：动作兜底不续命，在座上下文只由"看到人脸"刷新，
                # 否则离座后的环境噪音能靠这条链路无限自我确认
                return "in_seat", {**detail,
                                   "reason": "no_face_but_motion_continuous"}
            # 动作量太小（环境噪音）或太大（走动/光线变化）：不给在座
            return "unknown", {**detail, "reason": "motion_out_of_working_band"}
        return "unknown", {**detail, "reason": "motion_without_context"}

    # ③ 画面几乎不动 —— 累计确认期后判"无人"，避免 YuNet 偶发漏检导致误判
    _no_motion_streak += 1
    detail["no_motion_streak"] = _no_motion_streak
    confirm = int(cfg("vision", "absent_confirm_samples", default=2))
    if _no_motion_streak >= confirm:
        return "absent", {**detail, "reason": "no_face_no_motion_confirmed"}
    return "unknown", {**detail, "reason": "no_face_no_motion_first"}


def note_presence(presence):
    """保留给测试与外部调用：记录上一轮结果"""
    global _prev_presence
    _prev_presence = presence


def reset_vision_state():
    """清空视觉判定的历史（换摄像头位置、测试时用）"""
    global _prev_gray, _prev_luma, _prev_presence, _no_motion_streak, _last_in_seat_ts
    _prev_gray = None
    _prev_luma = None
    _prev_presence = None
    _no_motion_streak = 0
    _last_in_seat_ts = None


def detect_presence_ollama(image_bytes, question):
    """用本机 Ollama 视觉模型判断。返回 (presence, detail)"""
    import base64
    import urllib.error
    import urllib.request

    url = cfg("vision", "ollama_url", default="")
    model = cfg("vision", "ollama_model", default="")
    if not url or not model:
        return "unknown", {"reason": "ollama_not_configured"}

    # P2-⑫：focus.enabled 时让模型顺带判断"是否在专注工作"
    if cfg("focus", "enabled", default=False):
        prompt = (
            "这是一张办公桌前的摄像头截图。请只回答以下一个词："
            "SEATED（有人坐在桌前专注工作，看着电脑/工作区域）、"
            "SEATED_DISTRACTED（有人，但明显在玩手机等分心行为）、"
            "ABSENT（明显没人）、 "
            "UNCLEAR（看不清/画面全黑/被遮挡）。不要解释。"
        )
    else:
        prompt = (
            "这是一张办公桌前的摄像头截图。请只回答一个词之一："
            "SEATED（能看到有人坐在桌前工作）、"
            "ABSENT（明显没人）、 "
            "UNCLEAR（看不清/画面全黑/被遮挡）。不要解释。"
        )
    payload = {
        "model": model,
        "prompt": prompt,
        "images": [base64.b64encode(image_bytes).decode("ascii")],
        "stream": False,
    }
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        text = (body.get("response") or "").upper()
        # 注意顺序：先判 SEATED_DISTRACTED，它包含 SEATED 子串
        if "SEATED_DISTRACTED" in text:
            return "in_seat", {"focus": "distracted", "raw": text}
        if "SEATED" in text:
            return "in_seat", {"raw": text}
        if "ABSENT" in text:
            return "absent", {"raw": text}
        return "unknown", {"raw": text}
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
        log(f"Ollama 判定失败：{e}", "WARN")
        return "unknown", {"reason": "ollama_error", "error": str(e)}


# --------------------------------------------------------------------------
# 云端视觉 API（2026-09-16，产品决策：本机不跑 ollama，视觉判定走大厂便宜/免费 API）
# OpenAI 兼容协议（/chat/completions + image_url base64），实测可选：
#   智谱 GLM-4V-Flash（免费） / 阿里 qwen-vl-plus（~0.8厘/图） / 豆包 doubao-1.5-vision-lite
# 触发频率 = 拍照频率（5 分钟一张，白天 <100 张/天），成本可忽略。
# --------------------------------------------------------------------------

def _api_settings_load():
    """读 Dashboard 保存的视觉 API 设置（优先级高于 config.yaml）。"""
    try:
        with open(API_SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _api_settings_save(d):
    API_SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(API_SETTINGS_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(tmp, API_SETTINGS_FILE)


# ---------------- 设备侧热配置（拍摄位等，免烧录）----------------

def _device_settings_load():
    try:
        with open(DEVICE_SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _device_settings_save(d):
    DEVICE_SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(DEVICE_SETTINGS_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DEVICE_SETTINGS_FILE)


def _photo_positions():
    """生效的拍摄位：device_settings.json > 固件内置默认。带 ver 供设备增量同步。
    extras = 扩展拍摄位（2026-09-17 多拍摄位）：[{name,yaw,pitch}]，最多 3 个，
    只拍记录照不做判定，照片在 Dashboard 照片页按名字筛选。"""
    st = _device_settings_load()
    pos = st.get("photo_positions") or {}
    seat = pos.get("seat") or _PHOTO_POS_DEFAULTS["seat"]
    cup = pos.get("cup") or _PHOTO_POS_DEFAULTS["cup"]
    extras = []
    for e in (pos.get("extras") or [])[:3]:
        try:
            extras.append({"name": str(e.get("name", ""))[:10],
                           "yaw": int(e["yaw"]), "pitch": int(e["pitch"])})
        except (KeyError, TypeError, ValueError):
            continue
    return {
        "ver": int(st.get("ver", 0)),
        "seat": {"yaw": int(seat["yaw"]), "pitch": int(seat["pitch"])},
        "cup": {"yaw": int(cup["yaw"]), "pitch": int(cup["pitch"])},
        "extras": extras,
    }


def _power_save_cfg():
    """生效的空闲休眠配置：device_settings.json > 固件内置默认。"""
    st = _device_settings_load()
    ps = st.get("power_save") or {}
    enabled = bool(ps.get("enabled", _POWER_SAVE_DEFAULTS["enabled"]))
    minutes = int(ps.get("idle_sleep_minutes", _POWER_SAVE_DEFAULTS["idle_sleep_minutes"]))
    minutes = max(1, min(720, minutes))
    return {"enabled": enabled, "idle_sleep_minutes": minutes}


def _effective_api_cfg():
    """视觉 API 的生效配置：Dashboard 设置 > 环境变量 > config.yaml。
    返回 (base_url, model, api_key)。"""
    st = _api_settings_load()
    base_url = (st.get("base_url")
                or cfg("vision", "api", "base_url", default="") or "").strip()
    model = (st.get("model")
             or cfg("vision", "api", "model", default="") or "").strip()
    api_key = (st.get("api_key")
               or os.environ.get("STACKCHAN_VISION_API_KEY")
               or cfg("vision", "api", "api_key", default="") or "").strip()
    return base_url, model, api_key


def _effective_water_enabled():
    """喝水判定总开关：config.yaml 或 Dashboard 设置任一打开即算开。"""
    if cfg("water", "enabled", default=False):
        return True
    return bool(_api_settings_load().get("water_enabled", False))


def _vision_api_call(image_bytes, prompt, timeout=30):
    """调云端视觉 API。返回 (ok, text, err)。所有视觉类云判定共用这一个出口。"""
    import base64
    import urllib.error
    import urllib.request

    base_url, model, api_key = _effective_api_cfg()
    if not (base_url and model and api_key):
        return False, "", "api_not_configured"

    payload = {
        "model": model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url",
                 "image_url": {"url": "data:image/jpeg;base64,"
                                      + base64.b64encode(image_bytes).decode("ascii")}},
                {"type": "text", "text": prompt},
            ],
        }],
        "temperature": 0.1,
        # 2026-09-16 修：32 会被推理模型（deepseek-flash 实测 reasoning_tokens 吃满
        # max_tokens，content 为空 finish=length）全部烧掉。512 保证思考+回答都有空间。
        "max_tokens": 512,
    }
    # DeepSeek 系推理模型显式关思考：水位/在座判定是一词答案，思考纯属浪费且
    # 实测 reasoning_effort=none 时 reasoning_tokens=None（彻底不思考）。
    # 其他厂商不认识该字段一般会忽略；万一 400 由 except 兜底报错可见。
    if "deepseek" in (base_url or "").lower() or "deepseek" in (model or "").lower():
        payload["reasoning_effort"] = "none"
    req = urllib.request.Request(
        base_url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        text = (body.get("choices", [{}])[0].get("message", {}).get("content")
                or "").strip()
        return True, text, ""
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError,
            KeyError, IndexError) as e:
        return False, "", str(e)


def detect_presence_api(image_bytes, question):
    """云端 API 判在场。返回 (presence, detail)，结论词与 ollama 版一致。
    2026-09-16 行为分析立项：SEATED 时顺带返回坐姿（POSTURE），一次视觉调用
    两份产出，不增加请求次数。解析对旧格式（无前缀单词）保持兼容。"""
    if cfg("focus", "enabled", default=False):
        prompt = (
            "这是一张办公桌前的摄像头截图。请严格按以下格式回答，不要解释：\n"
            "PRESENCE:SEATED（有人坐在桌前，含分心）/ SEATED_DISTRACTED（有人但明显在玩手机等分心）/ "
            "ABSENT（明显没人）/ UNCLEAR（看不清/画面全黑/被遮挡）\n"
            "如果 PRESENCE 是 SEATED 或 SEATED_DISTRACTED，再输出一行：\n"
            "POSTURE:GOOD（坐姿端正）/ HUNCHING（明显驼背、低头含胸）/ SLUMPED（趴在桌上或快躺下）\n"
        )
    else:
        prompt = (
            "这是一张办公桌前的摄像头截图。请严格按以下格式回答，不要解释：\n"
            "PRESENCE:SEATED（能看到有人坐在桌前）/ ABSENT（明显没人）/ "
            "UNCLEAR（看不清/画面全黑/被遮挡）\n"
            "如果 PRESENCE 是 SEATED，再输出一行：\n"
            "POSTURE:GOOD（坐姿端正）/ HUNCHING（明显驼背、低头含胸）/ SLUMPED（趴在桌上或快躺下）\n"
        )
    ok, text, err = _vision_api_call(image_bytes, prompt)
    if not ok:
        log(f"云端视觉判定失败：{err}", "WARN")
        return "unknown", {"reason": "api_error", "error": err}
    text_u = text.upper()

    detail = {"raw": text_u}
    # 新格式：PRESENCE: 前缀
    m = re.search(r"PRESENCE\s*[:：]\s*(SEATED_DISTRACTED|SEATED|ABSENT|UNCLEAR)", text_u)
    if m:
        word = m.group(1)
        if word in ("SEATED", "SEATED_DISTRACTED"):
            presence = "in_seat"
            if word == "SEATED_DISTRACTED":
                detail["focus"] = "distracted"
        elif word == "ABSENT":
            presence = "absent"
        else:
            presence = "unknown"
    else:
        # 旧格式兼容：整段找关键词
        if "SEATED" in text_u:
            presence = "in_seat"
        elif "ABSENT" in text_u:
            presence = "absent"
        else:
            presence = "unknown"

    # 坐姿（有 PRESENCE 前缀的新格式才解析；旧格式没有 POSTURE 行）
    mp = re.search(r"POSTURE\s*[:：]\s*(GOOD|HUNCHING|SLUMPED)", text_u)
    if mp:
        detail["posture"] = mp.group(1).lower()
    return presence, detail


def _effective_vision_mode():
    """在座判定的生效模式（2026-09-16 起）：Dashboard 配齐视觉 API（厂商+模型+Key）
    即自动走云端判定，不再要求手动改 config.yaml 的 vision.mode——用户配了 Key
    却发现判定还是 YuNet 就是这个断点。API 不全时回退 config.yaml 的本地模式。"""
    base_url, model, api_key = _effective_api_cfg()
    if base_url and model and api_key:
        return "api"
    return (cfg("vision", "mode", default="opencv") or "opencv").lower()


def judge_presence(image_bytes, question):
    mode = _effective_vision_mode()
    if mode == "api":
        presence, detail = detect_presence_api(image_bytes, question)
        if detail.get("reason") != "api_error":
            return presence, detail
        # 云端失败（网络/额度/参数）自动降级本地 YuNet，宁可用简判不停摆
        log("云端在座判定失败，本轮降级本地 YuNet", "WARN")
        return detect_presence_opencv(image_bytes)
    if mode == "opencv":
        return detect_presence_opencv(image_bytes)
    if mode == "ollama":
        return detect_presence_ollama(image_bytes, question)
    return "unknown", {"reason": "vision_disabled"}


# --------------------------------------------------------------------------
# 喝水提醒（2026-09-16 健康陪伴路线 Phase 1）
# 设计（用户 09-15 拍板）：桥接=大脑，照片不出局域网；
# 规则引擎定"何时说"（节流+每日上限+人在工位才提醒），ollama 定"水位判定"。
# --------------------------------------------------------------------------

def judge_water(image_bytes):
    """判水杯水位。返回 (level, detail)。
    level: full（水还多，不提醒）/ low（喝了大半，可提醒）/ empty（空杯，该续水）
           / None（未判定）。
    judge_mode: off（骨架，直接 None）/ api（云端视觉，2026-09-16 起主推）
                / ollama（本机，部分设备内存紧张已弃用，保留兼容）。"""
    mode = (cfg("water", "judge_mode", default="off") or "off").lower()

    prompt = (
        "这是一张办公桌前的摄像头截图。请找到画面里的水杯/杯子，"
        "先判断杯中剩余水量档位，只回答以下一个词之一："
        "FULL（水还多，大半以上）、"
        "LOW（只剩不到三分之一，明显喝掉大半）、"
        "EMPTY（杯子空了或几乎空了）、"
        "NO_CUP（画面里找不到水杯）、"
        "UNCLEAR（看不清）。"
        "然后另起一行回答水位百分比估计，格式：WATER_PERCENT:数字（0~100，"
        "水位高度占杯身的百分比估计，看不到液面就给最接近的估计值）。不要解释。"
    )

    if mode == "api":
        ok, text, err = _vision_api_call(image_bytes, prompt)
        if not ok:
            log(f"喝水判定（云端 API）失败：{err}", "WARN")
            return None, {"reason": "api_error", "error": err}
        text = text.upper()
        for level in ("FULL", "LOW", "EMPTY", "NO_CUP", "UNCLEAR"):
            if level in text:
                detail = {"raw": text, "via": "api"}
                mw = re.search(r"WATER_PERCENT\s*[:：]\s*(\d{1,3})", text)
                if mw:
                    detail["percent"] = min(100, int(mw.group(1)))
                return level.lower(), detail
        return None, {"raw": text, "via": "api"}

    if mode != "ollama":
        return None, {"reason": f"water_judge_{mode}"}

    import base64
    import urllib.error
    import urllib.request

    url = cfg("vision", "ollama_url", default="")
    model = cfg("water", "ollama_model", default=cfg("vision", "ollama_model", default=""))
    if not url or not model:
        return None, {"reason": "ollama_not_configured"}

    payload = {
        "model": model,
        "prompt": prompt,
        "images": [base64.b64encode(image_bytes).decode("ascii")],
        "stream": False,
    }
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        text = (body.get("response") or "").upper()
        for level in ("FULL", "LOW", "EMPTY", "NO_CUP", "UNCLEAR"):
            if level in text:
                # 2026-09-16 修：原来返回大写，下游 handle_water_check 比较小写
                # low/empty → 永远不触发提醒。统一小写。
                return level.lower(), {"raw": text}
        return None, {"raw": text}
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
        log(f"喝水判定失败：{e}", "WARN")
        return None, {"reason": "ollama_error", "error": str(e)}


def handle_water_check(image_bytes, presence, photo_ts=None, photo_name=None):
    """/vision 收到照片后的喝水检测入口。异步线程里执行（云端 API 判定可能 2-10s，
    不能阻塞 /vision 响应）。规则：人在工位 + 水位 low/empty + 过冷却 + 未超每日上限。"""
    if not _effective_water_enabled():
        return
    if presence != "in_seat":
        return  # 宁漏不误：人不在工位不提醒

    level, detail = judge_water(image_bytes)

    # 分析流水（Dashboard 用）：水位判定结果
    _append_analysis({
        "type": "water",
        "ts": photo_ts or datetime.now().isoformat(timespec="seconds"),
        "file": photo_name,
        "level": level,
        "detail": detail if isinstance(detail, dict) else {"raw": str(detail)},
    })

    now = datetime.now()
    with _state_lock:
        STATE["water_level"] = level
        STATE["water_last_ts"] = now.isoformat(timespec="seconds")

        # ---- 行为分析 v1（产品立项）：水位时间序列 + "水位不动 30 分钟"检测 ----
        # 第一性原理：只需要"变没变、剩多少"的粗粒度信号，不需要毫米级精度；
        # 模型估计有噪声，把"显著变化"定为百分比 Δ≥10%。
        # 2026-09-16 修：no_cup/unclear 时模型会填 WATER_PERCENT:0 之类的垃圾值，
        # 污染曲线（19:00 出现假"陡降"）→ 只记录杯子真实在画面的轮次。
        percent = detail.get("percent") if isinstance(detail, dict) else None
        if percent is not None and level in ("full", "low", "empty"):
            series = [x for x in (STATE.get("water_series") or [])
                      if isinstance(x, list) and len(x) == 2]
            prev = series[-1][1] if series else None
            series.append([now.isoformat(timespec="seconds"), percent])
            # 保留最近 3 小时（5 分钟一轮 ≈ 36 点）
            STATE["water_series"] = series[-36:]
            if prev is None or abs(percent - prev) >= 10:
                STATE["water_last_change_ts"] = now.isoformat(timespec="seconds")

        if level not in ("low", "empty"):
            # 水位还多时才有"该喝水了"的需求；low/empty 走下面原有的续水提醒
            if level == "full" and percent is not None:
                w_last_change = _parse_ts(STATE.get("water_last_change_ts"))
                still_min = float(cfg("behavior", "water_still_minutes",
                                      default=30))
                if (w_last_change is not None
                        and (now - w_last_change).total_seconds()
                        >= still_min * 60):
                    ws_last = _parse_ts(STATE.get("water_still_last_remind_ts"))
                    ws_cd = float(cfg("behavior",
                                      "water_still_cooldown_minutes",
                                      default=60))
                    ws_limit = int(cfg("behavior", "water_still_daily_limit",
                                      default=4))
                    if (ws_last is None
                            or (now - ws_last).total_seconds() >= ws_cd * 60) \
                            and STATE.get("water_still_reminds_today", 0) < ws_limit:
                        STATE["water_still_last_remind_ts"] = (
                            now.isoformat(timespec="seconds"))
                        STATE["water_still_reminds_today"] = (
                            STATE.get("water_still_reminds_today", 0) + 1)
                        log(f"water: 水位 {percent}% 已 "
                            f"{still_min:.0f} 分钟无明显变化，触发喝水提醒")
                        if cfg("notify", "feishu", default=True):
                            threading.Thread(
                                target=notify_feishu,
                                args=(f"💧 喝水提醒：{now.strftime('%H:%M')} 水位"
                                      f" {percent}% 已约 {still_min:.0f} 分钟没变"
                                      f"化，该喝两口了。",),
                                daemon=True).start()
                        if cfg("behavior", "speak", default=True):
                            push_speak(
                                STATE,
                                cfg("behavior", "water_still_message",
                                    default="主人，有一阵子没喝水了，端起水杯喝两口吧。"),
                                source="water_still")
            save_state(STATE)
            log(f"water: level={level} percent={percent} detail={detail}（无需续水提醒）")
            return

        last = _parse_ts(STATE.get("water_last_remind_ts"))
        cooldown = float(cfg("water", "cooldown_minutes", default=60))
        daily_limit = int(cfg("water", "daily_limit", default=6))
        if (last is not None
                and (now - last).total_seconds() < cooldown * 60):
            log("water: 冷却期内，跳过提醒")
            save_state(STATE)
            return
        if STATE.get("water_reminds_today", 0) >= daily_limit:
            log("water: 今日提醒已达上限，跳过")
            save_state(STATE)
            return

        STATE["water_last_remind_ts"] = now.isoformat(timespec="seconds")
        STATE["water_reminds_today"] = STATE.get("water_reminds_today", 0) + 1
        save_state(STATE)

    level_text = "快喝完了" if level == "low" else "已经空了"
    msg = cfg("water", "remind_message",
              default=f"主人，水杯{level_text}，该喝水啦。")
    msg = msg.format(level_text=level_text)
    log(f"water: 触发提醒（level={level}）")
    if cfg("notify", "feishu", default=True):
        threading.Thread(
            target=notify_feishu,
            args=(f"💧 喝水提醒：{now.strftime('%H:%M')} 检测到水杯{level_text}。",),
            daemon=True).start()
    if cfg("water", "speak", default=True):
        push_speak(STATE, msg, source="water", count_daily=False)


def _behavior_summary():
    """行为分析状态（Dashboard 总览卡 + 趋势曲线数据源）。"""
    with _state_lock:
        series = [x for x in (STATE.get("water_series") or [])
                  if isinstance(x, list) and len(x) == 2]
        return {
            "posture_last": STATE.get("posture_last"),
            "posture_consecutive_hunch": STATE.get("posture_consecutive_hunch", 0),
            "posture_reminds_today": STATE.get("posture_reminds_today", 0),
            "water_percent": series[-1][1] if series else None,
            "water_series": series,
            "water_still_reminds_today": STATE.get("water_still_reminds_today", 0),
        }


@app.route("/api/dashboard/behavior", methods=["GET"])
def dashboard_behavior():
    """行为分析曲线数据：从 analysis.jsonl 提取在座/坐姿/水位三条序列（最多 200 条流水）。"""
    rows = _read_analysis_tail(limit=200)
    posture, water, presence = [], [], []
    for r in rows:
        ts = (r.get("ts") or "")
        if r.get("type") == "vision":
            po = (r.get("detail") or {}).get("posture")
            if po:
                posture.append({"ts": ts, "posture": po})
            presence.append({"ts": ts, "presence": r.get("presence")})
        elif r.get("type") == "water":
            # no_cup/unclear 轮次的 percent 是垃圾值（0），不进曲线
            lv = r.get("level") or (r.get("detail") or {}).get("level")
            if lv and lv not in ("full", "low", "empty"):
                continue
            pc = (r.get("detail") or {}).get("percent")
            if pc is not None:
                water.append({"ts": ts, "percent": pc})
    return jsonify({"ok": True,
                    "posture": posture[-100:],
                    "water": water[-100:],
                    "presence": presence[-100:],
                    "state": _behavior_summary()})


def describe_presence(presence, detail, minutes_in_seat):
    """返回给机器人读的一句话（会被云端 LLM 拿去组织语言）"""
    if presence == "in_seat":
        if minutes_in_seat is not None and minutes_in_seat >= 1:
            return (f"画面中检测到有人坐在工位前，本次已连续在座约 "
                    f"{int(minutes_in_seat)} 分钟。")
        return "画面中检测到有人坐在工位前。"
    if presence == "absent":
        return "画面中没有检测到人，工位看起来是空的。"
    reason = detail.get("reason", "")
    if reason == "too_dark":
        return "画面太暗或摄像头被遮挡，无法判断工位上有没有人。"
    return "画面里没有检测到人脸，无法确定是否有人在工位（可能背对镜头或有人但侧身）。"


# --------------------------------------------------------------------------
# 提醒策略
# --------------------------------------------------------------------------

def within_active_window():
    now = datetime.now()
    weekdays = cfg("presence", "active_weekdays", default=[1, 2, 3, 4, 5])
    # config 用 1=周一..7=周日；Python weekday() 是 0=周一..6=周日
    if (now.weekday() + 1) not in weekdays:
        return False
    hours = cfg("presence", "active_hours", default=[9, 18])
    try:
        start_h, end_h = int(hours[0]), int(hours[1])
    except (TypeError, IndexError, ValueError):
        start_h, end_h = 9, 18
    return start_h <= now.hour < end_h


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def evaluate_reminder(st, current_presence=None):
    """根据当前状态决定是否需要提醒。返回提醒文案或 None。"""
    if not cfg("presence", "enabled", default=True):
        return None
    if not within_active_window():
        return None

    # 只在明确确认"在座"时才提醒：
    #   absent  -> 人不在，别催他站起来
    #   unknown -> 画面太暗/看不清，宁可漏提醒也不要误提醒
    presence = current_presence or st.get("last_presence")
    if presence != "in_seat":
        return None

    since = _parse_ts(st.get("in_seat_since"))
    if since is None:
        return None

    # 提醒过则从 last_remind_ts 重新起算，阈值用 reset_to_minutes
    base = _parse_ts(st.get("last_remind_ts")) or since
    threshold = (cfg("presence", "reset_to_minutes", default=25)
                 if st.get("last_remind_ts")
                 else cfg("presence", "remind_after_minutes", default=30))

    if datetime.now() - base < timedelta(minutes=threshold):
        return None

    if st.get("reminders_today", 0) >= cfg("presence", "max_reminders_per_day",
                                           default=6):
        return None

    pool = cfg("presence", "messages", default=["该起身活动一下了。"])
    if not isinstance(pool, list) or not pool:
        pool = ["该起身活动一下了。"]
    return random.choice(pool)


def push_reminder(st, message):
    if not message:
        return
    st["last_remind_ts"] = datetime.now().isoformat(timespec="seconds")
    st["reminders_today"] = st.get("reminders_today", 0) + 1
    st["pending"].append({
        "type": "remind_standup",
        "message": message,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    })
    log(f"生成提醒（今日第 {st['reminders_today']} 次）：{message}")

    if cfg("notify", "feishu", default=True):
        threading.Thread(target=notify_feishu,
                         args=(f"⏰ 久坐提醒\n{message}",),
                         daemon=True).start()


def push_speak(st, message, source="external", count_daily=True):
    """往 pending 队列塞一条"让机器人开口"的指令（P2-⑨ 反向通道）

    与 push_reminder 的区别：不占久坐提醒的每日额度，有独立上限与队列上限。
    count_daily=False 时不受每日上限约束（用于任务回执：用户主动下指令后的
    结果播报属于"有问必答"，不该被防骚扰额度吞掉；队列满仍会拒）。
    返回 True 表示已入队。
    """
    if not message:
        return False
    if len(st.get("pending", [])) >= 20:
        log("pending 队列已满（20 条），丢弃新的播报请求", "WARN")
        return False
    if count_daily and st.get("speaks_today", 0) >= cfg(
            "calendar", "max_speaks_per_day", default=12):
        log("今日播报已达上限，丢弃", "WARN")
        return False
    # 2026-09-16 修：所有播报（含任务回执）都计入 speaks_today——
    # Dashboard"播报"卡片文案是"含任务回执"，但回执此前不计数（显示 0 次误导）。
    # count_daily 只控制"是否受每日上限约束"。
    st["speaks_today"] = st.get("speaks_today", 0) + 1
    if count_daily:
        log(f"生成播报（今日第 {st['speaks_today']} 次，来源 {source}）：{message[:80]}")
    else:
        log(f"生成播报（任务回执，不占每日额度，来源 {source}）：{message[:80]}")
    st["pending"].append({
        "type": "speak",
        "message": message,
        "source": source,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    })
    _tts_warm_async(message)
    return True


# ---------------- TTS 预热（2026-09-15 配套设备 v1.9.12） ----------------
# 设备取 /tts/url 时若冷缓存，桥接现场合成要 10~15s，可能超出设备 HTTP
# 超时（实测"叮咚了但没语音"就是这个：fetch failed → 只放兜底叮咚）。
# 播报一入队就后台预热缓存；设备最迟 30s 后才来轮询领取，届时直接命中。
_tts_warm_lock = threading.Lock()


def _clip_like_device(text):
    """模拟设备端 text.substr(0, 120)（120 字节；仅超长时才可能劈开多字节字符）。

    2026-09-15 修复：初版无论是否截断都做"尾部续字节回退"，把所有以中文
    结尾的文本最后一个字符剥掉了（如句号），导致预热 key 与设备请求 key
    永远对不上。现在只在真正超过 120 字节时才做边界回退。
    """
    b = (text or "").encode("utf-8")
    if len(b) <= 120:
        return (text or "")
    b = b[:120]
    while b and (b[-1] & 0xC0) == 0x80:
        b = b[:-1]
    return b.decode("utf-8", errors="ignore")


def _tts_warm_async(message):
    def _run():
        if not cfg("tts", "enabled", default=True):
            return
        text = _clip_like_device(message)
        if not text:
            return
        voice = cfg("tts", "voice", default="zh-CN-XiaoxiaoNeural")
        key = hashlib.sha1(f"{voice}:{text}".encode("utf-8")).hexdigest()[:16]
        out_path = TTS_DIR / f"{key}.ogg"
        if out_path.exists() and out_path.stat().st_size > 512:
            return  # 已有缓存
        if not _tts_warm_lock.acquire(blocking=False):
            return  # 已有合成在跑，放弃本次（设备端有重试兜底）
        try:
            TTS_DIR.mkdir(parents=True, exist_ok=True)
            if _tts_synthesize(text, voice, out_path):
                _tts_cleanup()
                log(f"TTS 预热：{out_path.name}（{out_path.stat().st_size}B，{voice}）")
        except Exception as e:
            log(f"TTS 预热失败：{e}", "WARN")
        finally:
            _tts_warm_lock.release()
    threading.Thread(target=_run, daemon=True).start()


# /notify 的短时去重缓存：同一内容 90 秒内只入队一次
_notify_seen = {}
_NOTIFY_DEDUPE_SECONDS = 90.0


def notify_feishu(text, subject=None):
    """通过 hermes send 推消息到飞书"""
    target = cfg("hermes", "feishu_target", default="")
    if not target:
        log("未配置 hermes.feishu_target，跳过飞书推送", "WARN")
        return False
    bin_path = cfg("hermes", "bin", default="hermes")
    args = [bin_path, "send", "--to", target]
    if subject:
        args += ["-s", subject]
    args.append(text)
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=60)
        if proc.returncode == 0:
            log("飞书推送成功")
            _FEISHU_LAST.update({"ts": datetime.now().isoformat(timespec="seconds"),
                                 "ok": True, "error": ""})
            return True
        err = proc.stderr.strip()[:300]
        log(f"飞书推送失败 rc={proc.returncode} err={err}", "WARN")
        _FEISHU_LAST.update({"ts": datetime.now().isoformat(timespec="seconds"),
                             "ok": False, "error": err[:120]})
    except (subprocess.TimeoutExpired, OSError) as e:
        log(f"飞书推送异常：{e}", "WARN")
        _FEISHU_LAST.update({"ts": datetime.now().isoformat(timespec="seconds"),
                             "ok": False, "error": str(e)[:120]})
    return False


def _device_offline_s():
    """设备心跳静默时长（秒）。从未见过设备时返回正无穷。"""
    ts = _DEVICE_LAST_SEEN.get("ts") or 0
    if not ts:
        return float("inf")
    return time.time() - ts


def monitor_loop():
    """兜底：即使没有新照片进来，也要按时间触发提醒。

    2026-10-10 设备离线守卫：设备心跳 >5 分钟无请求时（死机/断网/关机），
    久坐计时入账并暂停、不再生成提醒——否则监控线程会凭冻结的旧状态
    把整夜离线都累计成"在座"（实测刷出 688 分钟垃圾账），恢复后还会
    立刻误触久坐提醒。设备回来后由下一张判定照片重新起算。
    """
    while True:
        try:
            time.sleep(60)
            with _state_lock:
                reset_daily_if_needed(STATE)
                if _device_offline_s() > 300:
                    if STATE.get("in_seat_since"):
                        # 离线期间的时长无法核实，直接丢弃不入账
                        STATE["in_seat_since"] = None
                        STATE["last_remind_ts"] = None
                        STATE["offline_timer_paused"] = True
                        save_state(STATE)
                        log("设备心跳超时（>5 分钟无请求）：久坐计时暂停，"
                            "不再生成提醒（设备回来后由照片重新起算）")
                    continue
                if STATE.pop("offline_timer_paused", None):
                    log("设备心跳恢复：久坐计时重新可用")
                msg = evaluate_reminder(STATE)
                if msg:
                    push_reminder(STATE, msg)
                save_state(STATE)
        except Exception:  # noqa: BLE001
            log("monitor_loop 异常：\n" + traceback.format_exc(), "ERROR")


# --------------------------------------------------------------------------
# 日历播报（P2-⑨ 反向通道的电脑侧引擎）
# --------------------------------------------------------------------------

def parse_calendar_events(text):
    """把 fetch_cmd 的输出解析成事件列表。

    兼容三种格式：{"ok":true,"data":[...]}、裸 JSON 数组、NDJSON（每行一个对象）。
    解析不出任何东西就返回 []（宁可漏播也不误播）。
    """
    text = (text or "").strip()
    if not text:
        return []
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            data = obj.get("data")
            if isinstance(data, list):
                return data
            # 单个事件对象（没有 data 包裹）：长得像事件就当成一个事件
            if any(k in obj for k in _EVENT_START_KEYS):
                return [obj]
            return []
        if isinstance(obj, list):
            return obj
    except json.JSONDecodeError:
        pass
    events = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
            if isinstance(item, dict):
                events.append(item)
        except json.JSONDecodeError:
            continue
    return events


_EVENT_START_KEYS = ("start", "start_time", "begin", "starts_at")
_EVENT_END_KEYS = ("end", "end_time", "finish", "ends_at")
_EVENT_TITLE_KEYS = ("title", "summary", "subject", "name")
_EVENT_LOCATION_KEYS = ("location", "place", "room", "meeting_room")


def _event_field(ev, keys):
    for k in keys:
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)):
            return str(v)
    return ""


def _parse_event_time(value):
    """事件时间：ISO8601 / 'YYYY-MM-DD HH:MM' / epoch 秒。返回本地 naive datetime。"""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(float(value))
        except (OSError, OverflowError, ValueError):
            return None
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        try:
            return datetime.fromtimestamp(float(s))
        except (OSError, OverflowError, ValueError):
            return None
    s2 = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(s2)
    except ValueError:
        try:
            dt = datetime.strptime(s[:16], "%Y-%m-%d %H:%M")
        except ValueError:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def normalize_calendar_event(ev):
    """归一化成 {title, start, end, location}；缺开始时间的算无效事件"""
    if not isinstance(ev, dict):
        return None
    start = _parse_event_time(_event_field(ev, _EVENT_START_KEYS))
    if start is None:
        return None
    end = _parse_event_time(_event_field(ev, _EVENT_END_KEYS))
    title = _event_field(ev, _EVENT_TITLE_KEYS) or "未命名日程"
    location = _event_field(ev, _EVENT_LOCATION_KEYS)
    return {"title": title[:40], "start": start, "end": end, "location": location[:40]}


def select_announcements(events, now, lead_minutes, announced_keys, announce_hours):
    """挑出"此刻该播报"的日程。返回 [(key, message), ...]

    key = 标题@开始时间@提前量，写进 announced_events 防重复播报。
    """
    try:
        leads = sorted({int(x) for x in (lead_minutes or []) if int(x) >= 0},
                       reverse=True)
    except (TypeError, ValueError):
        leads = []
    if not leads:
        return []
    try:
        start_h, end_h = int(announce_hours[0]), int(announce_hours[1])
    except (TypeError, IndexError, ValueError):
        start_h, end_h = 8, 22

    out = []
    for ev in events:
        norm = normalize_calendar_event(ev)
        if norm is None or norm["start"] < now:
            continue
        for lead in leads:
            announce_at = norm["start"] - timedelta(minutes=lead)
            key = (f'{norm["title"]}@{norm["start"].isoformat(timespec="minutes")}'
                   f"@{lead}")
            if key in announced_keys:
                continue
            # 播报窗口：到点后 2 分钟内有效（服务忙过阵就不再补播）
            if not (announce_at <= now <= announce_at + timedelta(minutes=2)):
                continue
            if not (start_h <= now.hour < end_h):
                continue
            if lead == 0:
                msg = f'你现在有「{norm["title"]}」。'
            else:
                msg = f'{lead} 分钟后有「{norm["title"]}」'
                if norm["location"]:
                    msg += f'，地点 {norm["location"]}'
                msg += "。"
            out.append((key, msg))
            break  # 一个事件一轮最多播一次
    return out


def calendar_loop():
    """定期拉日历 → 快到点的日程推进 pending 队列，机器人到点开口播报"""
    while True:
        try:
            poll = int(cfg("calendar", "poll_seconds", default=60) or 60)
            time.sleep(max(20, poll))
            if not cfg("calendar", "enabled", default=False):
                continue
            # 会议模式（presence.enabled=False）期间保持安静
            if not cfg("presence", "enabled", default=True):
                continue
            cmd = (cfg("calendar", "fetch_cmd", default="") or "").strip()
            if not cmd:
                log("calendar.enabled=true 但未配置 fetch_cmd，跳过", "WARN")
                time.sleep(300)
                continue
            tz_now = datetime.now().astimezone()
            day_start = tz_now.replace(hour=0, minute=0, second=0, microsecond=0)
            run_cmd = (cmd.replace("{start}", day_start.isoformat())
                          .replace("{end}", (day_start + timedelta(days=1)).isoformat()))
            try:
                proc = subprocess.run(run_cmd, shell=True, capture_output=True,
                                      text=True, timeout=90)
                if proc.returncode != 0:
                    log(f"日历拉取失败 rc={proc.returncode}: "
                        f"{(proc.stderr or '')[:200]}", "WARN")
                    continue
                out = proc.stdout or ""
            except (subprocess.TimeoutExpired, OSError) as e:
                log(f"日历拉取异常：{e}", "WARN")
                continue

            events = parse_calendar_events(out)
            with _state_lock:
                reset_daily_if_needed(STATE)
                announced = set(STATE.get("announced_events", []))
                picks = select_announcements(
                    events, datetime.now(),
                    cfg("calendar", "lead_minutes", default=[15]),
                    announced,
                    cfg("calendar", "announce_hours", default=[8, 22]))
                for key, msg in picks:
                    if push_speak(STATE, msg, source="calendar"):
                        announced.add(key)
                        if (cfg("calendar", "feishu", default=True)
                                and cfg("notify", "feishu", default=True)):
                            threading.Thread(
                                target=notify_feishu,
                                args=(f"📅 日程播报\n{msg}",),
                                daemon=True).start()
                STATE["announced_events"] = list(announced)[-200:]
                save_state(STATE)
        except Exception:  # noqa: BLE001
            log("calendar_loop 异常：\n" + traceback.format_exc(), "ERROR")


# --------------------------------------------------------------------------
# 下班汇总（P2-⑩）与打卡（P2-⑭，打卡在 /vision 里触发）
# --------------------------------------------------------------------------

def compose_daily_summary(st, now=None):
    """拼当天汇总文案（纯函数，方便测试）"""
    now = now or datetime.now()
    first = _parse_ts(st.get("first_in_seat_ts"))
    last = _parse_ts(st.get("last_in_seat_ts"))
    secs = int(st.get("in_seat_seconds_today", 0) or 0)
    since = _parse_ts(st.get("in_seat_since"))
    if since:
        secs += max(0, int((now - since).total_seconds()))
    lines = [f"📊 今日工位小结（{now.strftime('%Y-%m-%d')}）"]
    lines.append(f"· 首次到工位：{first.strftime('%H:%M') if first else '今天还没有在座记录'}")
    if last:
        lines.append(f"· 最近在座：{last.strftime('%H:%M')}")
    if secs > 0:
        lines.append(f"· 累计在座：{secs // 3600} 小时 {(secs % 3600) // 60} 分钟")
    lines.append(f"· 久坐提醒：{st.get('reminders_today', 0)} 次")
    return "\n".join(lines)


def daily_summary_loop():
    """每天到点把当日小结推飞书（默认工作日 18:05）"""
    while True:
        try:
            time.sleep(30)
            if not cfg("daily_summary", "enabled", default=True):
                continue
            now = datetime.now()
            weekdays = cfg("daily_summary", "weekdays", default=[1, 2, 3, 4, 5])
            if (now.weekday() + 1) not in weekdays:
                continue
            hour = int(cfg("daily_summary", "hour", default=18))
            minute = int(cfg("daily_summary", "minute", default=5))
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if not (target <= now < target + timedelta(minutes=1)):
                continue
            with _state_lock:
                if STATE.get("summary_sent_date") == _today():
                    continue
                reset_daily_if_needed(STATE)
                text = compose_daily_summary(STATE, now)
                STATE["summary_sent_date"] = _today()
                save_state(STATE)
            if cfg("notify", "feishu", default=True):
                threading.Thread(target=notify_feishu, args=(text,),
                                 daemon=True).start()
                log("下班汇总已推送飞书")
        except Exception:  # noqa: BLE001
            log("daily_summary_loop 异常：\n" + traceback.format_exc(), "ERROR")


# --------------------------------------------------------------------------
# 路由
# --------------------------------------------------------------------------

def check_token():
    expected = cfg("security", "device_token", default="")
    if not expected:
        return True
    got = (request.headers.get("X-Device-Token")
           or request.form.get("token")
           or request.args.get("token")
           or "")
    return got == expected


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "time": datetime.now().isoformat(timespec="seconds"),
        "vision_mode": cfg("vision", "mode", default="opencv"),
        "presence_enabled": cfg("presence", "enabled", default=True),
    })


@app.route("/enroll", methods=["POST", "GET"])
def enroll():
    """主人脸注册：POST 一张单人照（multipart file= 或原始 JPEG），
    提取最大脸的 SFace 特征入库（data/user_face.npz）。

      POST /enroll                 追加一张模板
      POST /enroll?replace=1       清空后重新注册
      GET  /enroll?status=1        查看模板数量
    注册后立即生效，无需重启。
    """
    if not check_token():
        return "unauthorized", 401
    if request.args.get("status"):
        if _cv2_ready is None:
            _init_opencv()   # 懒加载未触发时模板数为 0，主动初始化一次
        return jsonify({"ok": True,
                        "templates": 0 if _user_templates is None
                        else int(len(_user_templates)),
                        "file": str(USER_FACE_FILE)})
    file_obj = request.files.get("file")
    image_bytes = file_obj.read() if file_obj is not None else request.get_data()
    if not image_bytes:
        return jsonify({"ok": False, "error": "没有收到图像数据"}), 400
    replace = request.args.get("replace") in ("1", "true", "yes")
    ok, info = _enroll_owner_face(image_bytes, replace=replace)
    if not ok:
        return jsonify({"ok": False, "error": str(info)}), 400
    log(f"主人脸注册成功：模板 {info['templates']} 张"
        + (f"，与已有模板自相似度 {info['self_sim']}" if info["self_sim"] is not None else ""))
    return jsonify({"ok": True, **info})


# ----------------------- 语音任务路由（2026-10-11 v2，MCP 版） -----------------------
# 固件 stt 钩子 + 规则分类器判定"复杂任务"→ 打断小智抢答 → POST /agent_task。
# 桥接把任务入队（不再往输入框注入），WorkBuddy 用协议方式取任务：
#   · MCP 工具：bridge_mcp_server.py（stdio，注册进 ~/.workbuddy/mcp.json），
#     get_agent_task 长轮询取单 / finish_agent_task 回传结果；
#   · 会话看门：_agent_watcher.py 后台长轮询 /agent_task/poll，取到单即退出，
#     WorkBuddy 收到后台任务通知后接单执行，干完 POST /agent_task/finish。
# 结果统一由桥接 push_speak → 小栗 TTS 播报。
# （v1 输入框注入废弃：锁屏/焦点不对就打进空气里，2026-10-11 两单全丢。）
# 电脑关机 = 固件 POST 失败 = 固件按简单任务放行给小智，天然回退。

AGENT_TASK_STATE = {"running": False, "status": "", "text": "", "id": 0,
                    "queued_at": 0.0, "picked_at": 0.0}
_AGENT_LOCK = threading.Lock()
_AGENT_NEW_TASK = threading.Event()
_AGENT_PICK_TTL_S = 120         # 派出后 120s 没被 WorkBuddy 取走 = 派送失败
_AGENT_TASK_TIMEOUT_S = 8 * 60  # 取走后 8 分钟没回结果 = 超时释放


def _agent_clip(text, limit=160):
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[:limit] + "……"


def _agent_maybe_expire():
    """惰性超时清理。返回 (kind, task_id) 表示刚发生一次超时需要播报。"""
    with _AGENT_LOCK:
        st = AGENT_TASK_STATE
        if not st["running"]:
            return None
        now = time.time()
        if st["status"] == "queued" and now - st["queued_at"] > _AGENT_PICK_TTL_S:
            st.update(running=False, status="")
            log(f"agent_task #{st['id']} 派送失败：{int(now - st['queued_at'])}s 无人取单", "WARN")
            return ("fail_pick", st["id"])
        if st["status"] == "picked" and now - st["picked_at"] > _AGENT_TASK_TIMEOUT_S:
            st.update(running=False, status="")
            log(f"agent_task #{st['id']} 执行超时（{int(now - st['picked_at'])}s）释放", "WARN")
            return ("fail_timeout", st["id"])
        return None


# （v1 的 _agent_inject/_agent_run 已删除：pyautogui 注入在锁屏或焦点
#  不在 WorkBuddy 时会把任务打进空气，且无法感知失败——2026-10-11 两单
#  全丢的根因。协议取单方案见文件头注释。）


# ---- 语音本地指令（2026-10-22）----
# 设备控制类口令在桥接本地执行、秒级语音确认，不派给 WorkBuddy（那是
# 秒级等待换分钟级往返，开关跟踪这种事没必要）。固件 IsComplexTask 的
# 关键词表已同步加入这些口令（esp_claw_board.cc），命中即路由到这里。
_TRACK_STOP_PATTERNS = (
    "退出跟随", "退出跟踪", "停止跟随", "停止跟踪",
    "关闭跟随", "关闭跟踪", "别跟着我", "不要跟着我",
    "别看我", "头转回去",
)


def _try_local_command(text):
    """设备控制类语音口令本地执行。命中返回语音回复文本，否则 None。"""
    for kw in _TRACK_STOP_PATTERNS:
        if kw in text:
            was_on = bool(TRACK.get("enabled") or TRACK.get("active"))
            tracking_stop_impl()  # 停跟 + 复位归位角（用户设定参数）
            log("语音本地指令命中「%s」：%s" % (kw, text[:60]))
            return ("好的，不跟啦，我把头转回去休息啦。"
                    if was_on else "我本来就没有在跟呀。")
    return None


@app.route("/agent_task", methods=["POST"])
def agent_task():
    """语音任务路由落点：固件判定复杂任务后转交 WorkBuddy。

      POST /agent_task   JSON {"text": 任务描述}
      返回 {"status": "accepted"|"busy"|"rejected", ...}
    accepted 时任务入队等待 WorkBuddy 取单（MCP 工具或看门脚本长轮询
    /agent_task/poll），并立即推接单回执给小栗播报。
    """
    if not check_token():
        return "unauthorized", 401
    expired = _agent_maybe_expire()
    if expired:
        _agent_speak_expire(expired)
    data = request.get_json(silent=True) or {}
    text = _agent_clip(str(data.get("text", "")), 600)
    if not text:
        return jsonify({"status": "rejected", "reason": "empty"}), 400
    # 语音本地指令（2026-10-22）：设备控制类口令本地秒回，不入任务队列。
    # 回 accepted 让固件收线小智会话，接单回执照常 ≤10s 播出。
    local_reply = _try_local_command(text)
    if local_reply is not None:
        push_speak(STATE, local_reply, source="voice_local", count_daily=False)
        return jsonify({"status": "accepted", "handled": "local"})
    with _AGENT_LOCK:
        if AGENT_TASK_STATE["running"]:
            log(f"agent_task 拒绝（上一单还没取走/做完）：{text[:50]}")
            return jsonify({"status": "busy",
                            "reason": "上一个任务还在执行中"})
        AGENT_TASK_STATE.update(running=True, status="queued", text=text,
                                id=AGENT_TASK_STATE["id"] + 1,
                                queued_at=time.time(), picked_at=0.0)
        task_id = AGENT_TASK_STATE["id"]
    log(f"agent_task #{task_id} 入队待取：{text[:60]}")
    push_speak(STATE, "好的，我让 WorkBuddy 来处理这个任务，结果出来我叫你。",
               source="agent_task", count_daily=False)
    _AGENT_NEW_TASK.set()  # 唤醒正在长轮询的取单方
    return jsonify({"status": "accepted", "task_id": task_id})


@app.route("/agent_task/poll", methods=["GET"])
def agent_task_poll():
    """WorkBuddy 取单长轮询（MCP server / 看门脚本共用）。

      GET /agent_task/poll?wait=45   最多等 wait 秒（上限 60）
    返回 {"task": null} 或 {"task": {"id": 3, "text": "..."}}。
    取走即置 picked，等 finish 回传；queued 超过 PICK_TTL 未取走会
    在惰性清理里播报失败并释放。
    """
    if not check_token():
        return "unauthorized", 401
    try:
        wait_s = max(0, min(int(request.args.get("wait", "45")), 60))
    except ValueError:
        wait_s = 45
    deadline = time.time() + wait_s
    task = None
    while True:
        expired = _agent_maybe_expire()
        if expired:
            _agent_speak_expire(expired)
        with _AGENT_LOCK:
            st = AGENT_TASK_STATE
            if st["running"] and st["status"] == "queued":
                st.update(status="picked", picked_at=time.time())
                task = {"id": st["id"], "text": st["text"]}
                log(f"agent_task #{st['id']} 已被 WorkBuddy 取走：{st['text'][:60]}")
                break
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        _AGENT_NEW_TASK.wait(min(remaining, 2.0))
        _AGENT_NEW_TASK.clear()
    return jsonify({"task": task})


@app.route("/agent_task/finish", methods=["POST"])
def agent_task_finish():
    """WorkBuddy 干完活回传结果：summary 会被小栗直接播报。

      POST /agent_task/finish  JSON {"task_id": 3, "summary": "播报文本",
                                     "success": true}
    """
    if not check_token():
        return "unauthorized", 401
    expired = _agent_maybe_expire()
    if expired:
        _agent_speak_expire(expired)
    data = request.get_json(silent=True) or {}
    try:
        task_id = int(data.get("task_id", 0))
    except (TypeError, ValueError):
        task_id = 0
    summary = _agent_clip(str(data.get("summary", "")).strip(), 200)
    success = bool(data.get("success", True))
    with _AGENT_LOCK:
        st = AGENT_TASK_STATE
        matched = st["running"] and st["id"] == task_id
        if matched:
            st.update(running=False, status="")
    if not matched:
        log(f"agent_task finish #{task_id} 无匹配的在办任务", "WARN")
        return jsonify({"ok": False, "reason": "no matching outstanding task"})
    if not summary:
        summary = "任务完成了。" if success else "抱歉，这个任务没做成。"
    log(f"agent_task #{task_id} 完成（success={success}）：{summary[:60]}")
    push_speak(STATE, summary, source="agent_task", count_daily=False)
    return jsonify({"ok": True})


def _agent_speak_expire(expired):
    kind, _tid = expired
    msg = ("WorkBuddy 这会儿没接单，任务先放下了，你可以再说一次试试。"
           if kind == "fail_pick" else
           "WorkBuddy 那边的任务做超时了，这一单先放下。")
    push_speak(STATE, msg, source="agent_task", count_daily=False)


# ============================================================
# 人脸跟踪（2026-10-14 舵机云台 v1）
# 设备把摄像头装在 SG90 摆臂上，以 3-4fps 上传 X-Photo-Type: track 帧；
# 本端跑 YuNet 找最大人脸的水平偏移，比例控制折算成目标角度，
# 随 /vision 响应回传 {"servo": 角度}——一帧一闭环，无需额外控制通道。
# 开关经 pending 队列下发（/tracking/start|stop → 设备 /pending 领单）。
# ============================================================
_track_lock = threading.Lock()
TRACK = {
    "enabled": False,
    "active": False,      # 设备端是否真在跟（2026-10-16 待机机制：丢脸超时→False）
    "angle": 90,          # 当前目标水平角度（桥接维护，帧间平滑）
    "tilt": 90,           # 当前目标俯仰角度（2026-10-16 二维云台）
    "last_face_ts": 0.0,  # 最近一次见到人脸的时刻（丢脸超时回中用）
    "last_frame_ts": 0.0,
    "frames": 0,
    "face_frames": 0,
    "start_ts": None,
    # ---- 安全保障（2026-10-21，俯仰舵机烧毁事故配套）----
    "pin_pan": None,      # 水平轴限位钉死计时 {"since": ts, "dir": "+"/"-"}
    "pin_tilt": None,     # 俯仰轴限位钉死计时
    "breaker": None,      # 熔断记录 {"reason","count","lockout","ts"}
    "trip_count": 0,      # 连续熔断次数（10 分钟窗口内累计，/tracking/start 清零）
    "last_trip_ts": 0.0,  # 上次熔断时刻（自动重启冷却用）
    "owner_sim": None,    # 当前跟踪目标的主人识别相似度（follow_owner_only）
    "target": None,       # 目标锁定：当前跟的脸中心（归一化），防多人在场来回跳
}


def _safety_cfg():
    """舵机安全保护参数（2026-10-21）"""
    return {
        # 目标角连续钉死限位超过该秒数 → 判定疑似堵转，熔断停跟踪
        "stall_limit_s": float(cfg("tracking", "stall_limit_s", default=10.0)),
        # 手动 /servo 指令是否夹到软限位内（标定时可临时关）
        "servo_clamp": bool(cfg("tracking", "servo_clamp", default=True)),
        # 熔断时是否语音播报告警
        "safety_speak": bool(cfg("tracking", "safety_speak", default=True)),
        # 熔断后自动重启冷却（秒）：给瞬态原因消散的时间，防重启即熔断连击
        "restart_cooldown_s": float(cfg("tracking", "restart_cooldown_s",
                                        default=60.0)),
        # 10 分钟内连续熔断达到该次数 → 锁死，不再自动恢复，需手动重启
        "trip_lockout": int(cfg("tracking", "trip_lockout", default=3)),
        # 跟踪只跟主人脸（SFace 验身份）
        "follow_owner_only": bool(cfg("tracking", "follow_owner_only",
                                      default=True)),
    }


def _pin_update(key, direction):
    """限位钉死连击计时：direction 为空=本帧未钉死（清零）；
    返回已连续钉死的秒数（未钉死返回 0）。"""
    now = time.time()
    st = TRACK.get(key)
    if not direction:
        TRACK[key] = None
        return 0.0
    if not st or st.get("dir") != direction:
        TRACK[key] = {"since": now, "dir": direction}
        return 0.0
    return now - st["since"]


def _trip_breaker(axis, direction, duration):
    """舵机堵转熔断（2026-10-21）：停跟踪 + 回安全姿态 + 告警。

    两级设计（2026-10-21 用户定稿）：归位角就是对着座位的视角，回正后
    要靠在座判定看到主人回来才能自动重启跟踪——所以熔断默认**不**关掉
    自动恢复：
    - 软熔断（连续 <3 次）：停跟踪回正，tracking_enabled 保持开 →
      在座判定看到主人回来自动重启（60s 冷却）。每次堵转被熔断限制在
      ≤10s 且随即回正卸力，单次无害。
    - 锁死（10 分钟内连续 3 次熔断 = 硬件性故障特征，如舵盘打滑/卡死）：
      tracking_enabled 持久化关，不再自动恢复，必须手动 /tracking/start。
    事故背景（2026-10-18 俯仰舵机烧毁）：舵盘在输出轴打滑产生恒定偏差 →
    闭环永远追不中 → 目标角被顶死在限位 → 满电流堵转 ~4 分钟 → 烧机。
    """
    tc = _track_cfg()
    sc = _safety_cfg()
    now = time.time()
    last = TRACK.get("last_trip_ts") or 0.0
    count = TRACK.get("trip_count") or 0
    count = count + 1 if (last and now - last < 600) else 1
    lockout = count >= sc["trip_lockout"]
    TRACK["trip_count"] = count
    TRACK["last_trip_ts"] = now
    TRACK["active"] = False
    TRACK["breaker"] = {
        "reason": "stall_%s" % axis,
        "count": count,
        "lockout": lockout,
        "ts": datetime.now().isoformat(timespec="seconds"),
    }
    if lockout:
        TRACK["enabled"] = False
        with _state_lock:
            STATE["tracking_enabled"] = False  # 持久化：重启后也不自动恢复
            save_state(STATE)
    _push_pending_command("tracking", "off")
    # 清零钉死计时：连续熔断按"故障回合"计（每回合都要重新钉满 10s），
    # 否则帧流还在时会按帧连击，1 秒内凑满 3 次误锁死（2026-10-21 实录）
    TRACK["pin_pan"] = None
    TRACK["pin_tilt"] = None
    TRACK["target"] = None
    # 回归安全姿态=归位视角（与待机回中同一组角度）：既让舵机离开限位
    # 解除堵转受力，又让摄像头对准座位，供在座判定自动重启
    _push_pending_command("servo", "%d,%d" % (tc["home_pan"], tc["home_tilt"]))
    detail = ("%s 轴目标角钉死限位（方向 %s）超过 %.0fs，疑似堵转/舵盘打滑/"
              "结构件卡死。已停跟踪并回正（pan=%d tilt=%d）。"
              % (axis, direction, duration, tc["home_pan"], tc["home_tilt"]))
    if lockout:
        log("舵机安全保护【锁死】：%s10 分钟内第 %d 次熔断，判定为持续性硬件"
            "故障，不再自动恢复；确认硬件正常后请用 Dashboard 或 "
            "POST /tracking/start 手动重启。" % (detail, count), "ERROR")
        speak_msg = ("主人，舵机连续三次保护触发，跟踪已锁死，快来检查一下云台。")
    else:
        log("舵机安全保护触发（第 %d/%d 次）：%s%.0f 秒后冷却结束，"
            "看到主人回座会自动重启跟踪。"
            % (count, sc["trip_lockout"], detail, sc["restart_cooldown_s"]),
            "ERROR")
        speak_msg = "主人，舵机保护触发了，我先回正休息一下，你坐好了我再接着看你。"
    if sc["safety_speak"]:
        try:
            push_speak(STATE, speak_msg, source="safety", count_daily=False)
        except Exception as exc:
            log("安全告警播报入队失败: %s" % exc, "WARN")
    if cfg("notify", "feishu", default=True):
        threading.Thread(target=notify_feishu,
                         args=("🚨 小栗舵机安全告警\n" + detail,),
                         daemon=True).start()


def _safety_startup_check():
    """启动自检（2026-10-21）：软限位必须包住归位姿态，否则跟踪一开环
    就会把舵机往限位顶。有问题返回 False，调用方应拒绝自动恢复跟踪。"""
    tc = _track_cfg()
    sc = _safety_cfg()
    problems = []
    if not tc["min_angle"] < tc["home_pan"] < tc["max_angle"]:
        problems.append("水平: home_pan=%d 不在行程 [%d, %d] 内"
                        % (tc["home_pan"], tc["min_angle"], tc["max_angle"]))
    if not tc["tilt_min_angle"] < tc["home_tilt"] < tc["tilt_max_angle"]:
        problems.append("俯仰: home_tilt=%d 不在行程 [%d, %d] 内"
                        % (tc["home_tilt"], tc["tilt_min_angle"],
                           tc["tilt_max_angle"]))
    for p in problems:
        log("舵机安全自检不通过：" + p, "ERROR")
    log("舵机安全保护：堵转熔断 %.0fs / 手动指令限幅 %s / 水平行程 [%d, %d] / "
        "俯仰行程 [%d, %d]"
        % (sc["stall_limit_s"], "开" if sc["servo_clamp"] else "关",
           tc["min_angle"], tc["max_angle"],
           tc["tilt_min_angle"], tc["tilt_max_angle"]))
    return not problems


def _track_cfg():
    return {
        "gain": float(cfg("tracking", "gain", default=14.0)),
        "max_step": float(cfg("tracking", "max_step", default=10.0)),
        "deadband": float(cfg("tracking", "deadband", default=0.06)),
        "min_angle": int(cfg("tracking", "min_angle", default=40)),
        "max_angle": int(cfg("tracking", "max_angle", default=140)),
        "return_delay": float(cfg("tracking", "return_delay_s", default=5.0)),
        # 待机（2026-10-16 用户方案）：持续丢脸超过该秒数就停掉跟踪回中，
        # 由在座判定负责看到主人回来再自动重启
        "standby_delay": float(cfg("tracking", "standby_delay_s", default=30.0)),
        # 归位姿态（2026-10-17 用户定）：丢脸回中/待机的目标角度，
        # 与固件 CLAW_SERVO_HOME_PAN/TILT 保持一致
        # 2026-10-23 起 Dashboard 可改（state.json 覆盖层优先，config.yaml
        # 作默认值）——cfg() 是启动缓存，改 yaml 必须重启，覆盖层免重启。
        "home_pan": int((STATE.get("tracking_home") or {}).get(
            "pan", cfg("tracking", "home_pan", default=100))),
        "home_tilt": int((STATE.get("tracking_home") or {}).get(
            "tilt", cfg("tracking", "home_tilt", default=110))),
        "flip": bool(cfg("tracking", "flip", default=False)),
        # 俯仰轴（2026-10-16 二维云台）：脸在画面下方 → 摄像头低头；
        # 方向装反改 tilt_flip。俯仰行程默认 60-120，防撞云台结构件。
        "tilt_gain": float(cfg("tracking", "tilt_gain", default=12.0)),
        "tilt_flip": bool(cfg("tracking", "tilt_flip", default=False)),
        "tilt_min_angle": int(cfg("tracking", "tilt_min_angle", default=60)),
        "tilt_max_angle": int(cfg("tracking", "tilt_max_angle", default=120)),
        # 只跟主人脸（2026-10-22）：跟踪目标必须过 SFace 验身份，防主人
        # 离开后追着隔壁桌同事走。SFace 模型不可用时退回旧逻辑（跟最大脸）
        "follow_owner_only": bool(cfg("tracking", "follow_owner_only",
                                      default=True)),
    }


def tracking_compute_angle(image_bytes):
    """对一帧 track 照片跑 YuNet，返回 (水平角度, 俯仰角度)（带锁串行化）。"""
    import cv2
    import numpy as np

    tc = _track_cfg()
    now = time.time()
    with _track_lock:
        # 非 active 期间（待机/熔断回正/已停）闭环短路：任何残留 track 帧
        # 只回归位角。防钉死角度继续下发、与回正指令打架（2026-10-21 修）。
        if not TRACK.get("active"):
            angle = int(max(tc["min_angle"],
                            min(tc["max_angle"], tc["home_pan"])))
            tilt = int(max(tc["tilt_min_angle"],
                           min(tc["tilt_max_angle"], tc["home_tilt"])))
            TRACK["angle"] = angle
            TRACK["tilt"] = tilt
            TRACK["last_frame_ts"] = now
            return angle, tilt
        angle = TRACK["angle"]
        tilt = TRACK["tilt"]
        if _init_opencv() and _yunet is not None:
            arr = np.frombuffer(image_bytes, dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img is not None:
                h, w = img.shape[:2]
                _yunet.setInputSize((w, h))
                result = _yunet.detect(img)
                dets = result[1] if isinstance(result, tuple) and len(result) > 1 else result
                d = None
                if dets is not None and len(dets):
                    # 选跟踪目标。默认最大脸（离得最近的人）；
                    # follow_owner_only（2026-10-22 用户反馈"我不在时它追
                    # 隔壁桌同事"——原逻辑不验身份，主人一走就跟着别人转）
                    # 开启时只跟 SFace 认出的主人脸：按脸大小降序最多验
                    # 3 张控制 CPU 开销，全都认不出 → 视同丢脸，走回正待机
                    d = max(dets, key=lambda r: float(r[2]) * float(r[3]))
                    if tc["follow_owner_only"] and _sface is not None \
                            and _user_templates is not None:
                        thr = float(cfg("vision", "recog_threshold",
                                        default=0.363))
                        # 迟滞门槛（2026-10-22 访客在场来回跳的修复）：已锁定
                        # 的目标过 thr 就继续跟；重新获取目标必须过更高门槛，
                        # 防陌生人 0.38 量级的误匹配抢走跟踪
                        acquire_thr = float(cfg(
                            "tracking", "recog_acquire_threshold",
                            default=0.45))
                        min_px = float(cfg("vision", "recog_min_face_px",
                                           default=60))
                        ranked = sorted(
                            dets, key=lambda r: float(r[2]) * float(r[3]),
                            reverse=True)[:3]
                        # 隔帧识别（2026-10-23）：SFace 每张脸 ~15-20ms，3fps
                        # 闭环里占掉约 10% 帧预算。改为隔帧验身份，中间帧用
                        # 目标锁定位置补间（0.12 半径内就近认领），平均省一半。
                        tick = (TRACK.get("recog_tick") or 0) + 1
                        TRACK["recog_tick"] = tick
                        tgt_now = TRACK.get("target")
                        do_recog = (tick % 2 == 1) or (tgt_now is None)
                        d = None
                        best = None
                        if do_recog:
                            owner_hits = []  # (sim, cand)
                            for cand in ranked:
                                if float(cand[2]) < min_px:
                                    continue  # 脸太小特征不可靠，跳过
                                try:
                                    aligned = _sface.alignCrop(img, cand)
                                    feat = np.asarray(_sface.feature(aligned),
                                                      dtype=np.float32).reshape(-1)
                                    best = max(_cosine_sim(feat, t)
                                               for t in _user_templates)
                                except Exception:  # noqa: BLE001
                                    continue
                                if best >= thr:
                                    owner_hits.append((best, cand))
                            best = None
                            if owner_hits:
                                tgt = TRACK.get("target")
                                if tgt:
                                    # 目标锁定：优先跟上一帧锁定的那张脸
                                    # （中心位置最近者），而不是每帧全局重选
                                    def _dist(c):
                                        cxn = (float(c[0]) + float(c[2]) / 2.0) / w
                                        cyn = (float(c[1]) + float(c[3]) / 2.0) / h
                                        return ((cxn - tgt["cx"]) ** 2
                                                + (cyn - tgt["cy"]) ** 2) ** 0.5
                                    sim0, cand0 = min(owner_hits,
                                                      key=lambda t: _dist(t[1]))
                                    if _dist(cand0) <= 0.3:
                                        d, best = cand0, sim0
                                if d is None:
                                    # 无锁定/锁定丢失：重新获取，用高门槛挑最像的
                                    strong = [t for t in owner_hits
                                              if t[0] >= acquire_thr]
                                    if strong:
                                        # 2026-10-27：连续 2 帧都过门槛才确认，
                                        # 单帧擦线误匹配（路过的人）拿不走跟踪
                                        streak = (TRACK.get("acq_streak")
                                                  or 0) + 1
                                        TRACK["acq_streak"] = streak
                                        if streak >= 2:
                                            best, d = max(strong,
                                                          key=lambda t: t[0])
                                            TRACK["acq_streak"] = 0
                                    else:
                                        TRACK["acq_streak"] = 0
                        else:
                            # 补间帧：不跑 SFace，离锁定点 0.12 内最近的脸
                            # 视为同一目标（下一帧仍会验身份纠错）
                            best_cand, best_dist = None, 1e9
                            for cand in ranked:
                                cxn = (float(cand[0]) + float(cand[2]) / 2.0) / w
                                cyn = (float(cand[1]) + float(cand[3]) / 2.0) / h
                                dist = ((cxn - tgt_now["cx"]) ** 2
                                        + (cyn - tgt_now["cy"]) ** 2) ** 0.5
                                if dist < best_dist:
                                    best_cand, best_dist = cand, dist
                            if best_cand is not None and best_dist <= 0.12:
                                d = best_cand
                                best = TRACK.get("owner_sim") or thr
                        if d is not None:
                            TRACK["owner_sim"] = round(best, 3)
                            TRACK["target"] = {
                                "cx": (float(d[0]) + float(d[2]) / 2.0) / w,
                                "cy": (float(d[1]) + float(d[3]) / 2.0) / h,
                            }
                        else:
                            TRACK["owner_sim"] = None
                            TRACK["target"] = None
                if d is not None:
                    cx = float(d[0]) + float(d[2]) / 2.0
                    cy = float(d[1]) + float(d[3]) / 2.0
                    offset = (cx - w / 2.0) / (w / 2.0)  # -1.0 .. 1.0
                    if tc["flip"]:
                        offset = -offset
                    if abs(offset) > tc["deadband"]:
                        step = max(-tc["max_step"],
                                   min(tc["max_step"], offset * tc["gain"]))
                        angle += step
                    # 俯仰：脸偏下（cy > h/2）→ offset_y > 0 → 低头
                    offset_y = (cy - h / 2.0) / (h / 2.0)
                    if tc["tilt_flip"]:
                        offset_y = -offset_y
                    if abs(offset_y) > tc["deadband"]:
                        step_y = max(-tc["max_step"],
                                     min(tc["max_step"], offset_y * tc["tilt_gain"]))
                        tilt += step_y
                    TRACK["last_face_ts"] = now
                    TRACK["face_frames"] += 1
                elif TRACK["last_face_ts"] and \
                        now - TRACK["last_face_ts"] > tc["return_delay"]:
                    # 丢脸超时：每帧双轴各回 2°，滑向归位姿态（默认 pan100/tilt80）
                    hp, ht = tc["home_pan"], tc["home_tilt"]
                    if angle > hp:
                        angle = max(float(hp), angle - 2)
                    elif angle < hp:
                        angle = min(float(hp), angle + 2)
                    if tilt > ht:
                        tilt = max(float(ht), tilt - 2)
                    elif tilt < ht:
                        tilt = min(float(ht), tilt + 2)
                    # 待机（2026-10-16 用户方案）：丢脸超过 standby_delay，
                    # 停掉跟踪回中省电，交给在座判定负责唤醒
                    if TRACK.get("active") and \
                            now - TRACK["last_face_ts"] > tc["standby_delay"]:
                        TRACK["active"] = False
                        _push_pending_command("tracking", "off")
                        log("人脸跟踪：持续未见人脸 %.0fs，转待机回中"
                            "（在座判定看到主人回来会自动重启）"
                            % (now - TRACK["last_face_ts"]), "WARN")
        # ---- 安全保障（2026-10-21）：限位钉死检测 ----
        # 限幅前的目标角越过软限位且持续 stall_limit_s → 疑似堵转（舵盘打滑/
        # 结构件顶死/舵机失效），熔断停跟踪。正常跟踪脸居中后即退出钉死，
        # 不会误触发；真被顶住时 10s 内断闭环，远短于烧机所需的分钟级堵转。
        sc = _safety_cfg()
        pin_p = ("+" if angle > tc["max_angle"] + 0.3
                 else "-" if angle < tc["min_angle"] - 0.3 else "")
        pin_t = ("+" if tilt > tc["tilt_max_angle"] + 0.3
                 else "-" if tilt < tc["tilt_min_angle"] - 0.3 else "")
        dur_p = _pin_update("pin_pan", pin_p)
        dur_t = _pin_update("pin_tilt", pin_t)
        if dur_p >= sc["stall_limit_s"]:
            _trip_breaker("pan", pin_p, dur_p)
        elif dur_t >= sc["stall_limit_s"]:
            _trip_breaker("tilt", pin_t, dur_t)

        angle = int(max(tc["min_angle"],
                        min(tc["max_angle"], round(angle))))
        tilt = int(max(tc["tilt_min_angle"],
                       min(tc["tilt_max_angle"], round(tilt))))
        TRACK["angle"] = angle
        TRACK["tilt"] = tilt
        TRACK["frames"] += 1
        TRACK["last_frame_ts"] = now
        return angle, tilt


def _push_pending_command(ctype, message):
    """往 pending 队列塞一条设备指令（跟踪开关等非播报类）。"""
    with _state_lock:
        if len(STATE.get("pending", [])) >= 20:
            log("pending 队列已满，指令丢弃", "WARN")
            return False
        STATE["pending"].append({
            "type": ctype,
            "message": message,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        })
    return True


def _push_servo_preview(pan, tilt):
    """Dashboard 拖滑杆时的角度预览（2026-10-23）。

    先清掉队列里还没被设备领走的 servo 指令，再压一条最新的——
    否则拖动几秒会积一串过时角度，设备按序执行时头会来回甩。
    """
    with _state_lock:
        STATE["pending"] = [p for p in STATE.get("pending", [])
                            if p.get("type") != "servo"]
    return _push_pending_command("servo", "%d,%d" % (pan, tilt))


@app.route("/tracking/start", methods=["POST"])
def tracking_start():
    tc = _track_cfg()
    TRACK["enabled"] = True
    TRACK["active"] = True
    TRACK["angle"] = tc["home_pan"]
    TRACK["tilt"] = tc["home_tilt"]
    TRACK["last_face_ts"] = 0.0
    TRACK["last_frame_ts"] = time.time()  # 保活基准：给首帧 90s 宽限，不立即补发
    TRACK["frames"] = 0
    TRACK["face_frames"] = 0
    TRACK["start_ts"] = datetime.now().isoformat(timespec="seconds")
    TRACK["pin_pan"] = None
    TRACK["pin_tilt"] = None
    TRACK["trip_count"] = 0
    TRACK["last_trip_ts"] = 0.0
    TRACK["breaker"] = None  # 手动开启视为已确认硬件正常，清除熔断记录
    with _state_lock:
        STATE["tracking_enabled"] = True  # 持久化：桥接重启后自动恢复
        save_state(STATE)
    ok = _push_pending_command("tracking", "on")
    log("人脸跟踪：已开启" + ("，指令已下发" if ok else "（pending 队列满，稍后设备重试领取期间照常工作会延迟）"))
    return jsonify({"ok": True, "queued": ok})


def tracking_stop_impl():
    """停跟踪并复位到归位姿态（用户语音口令 / 手动停止共用）。

    2026-10-22 用户要求：退出跟踪后舵机必须回到设定参数（home_pan/
    home_tilt，即对着座位的视角），不能停在最后跟踪角度。
    """
    tc = _track_cfg()
    TRACK["enabled"] = False
    TRACK["active"] = False
    TRACK["pin_pan"] = None
    TRACK["pin_tilt"] = None
    TRACK["target"] = None
    TRACK["angle"] = tc["home_pan"]
    TRACK["tilt"] = tc["home_tilt"]
    with _state_lock:
        STATE["tracking_enabled"] = False  # 持久化：桥接重启后不再自动恢复
        save_state(STATE)
    _push_pending_command("tracking", "off")
    # 复位归位姿态：先 off 停跟（设备收线断流），随后 servo 指令转回归位角
    _push_pending_command("servo", "%d,%d" % (tc["home_pan"], tc["home_tilt"]))


@app.route("/tracking/stop", methods=["POST"])
def tracking_stop():
    tracking_stop_impl()
    log("人脸跟踪：已停止并复位归位（pan=%d tilt=%d）"
        % (_track_cfg()["home_pan"], _track_cfg()["home_tilt"]))
    return jsonify({"ok": True, "homed": True})


def _tracking_keepalive():
    """跟踪保活 + 待机探测（2026-10-16）。

    - active（设备在跟）：>90s 无 track 帧（正常时无脸也持续传帧，无帧=
      跟踪停了或掉线，如 track_fail>8 自动停 / 设备重启）→ 补发 "on"；
      设备端重复收到 "on" 是 no-op，正在跟时无副作用。
    - 待机（丢脸超时已停，等在座判定唤醒）：每 2 分钟兜底塞一张拍照单，
      加速在座判定。哨兵"回归"事件本来就会立即派复核照（秒级），这里
      只是哨兵没触发时的兜底。
    """
    tick = 0
    while True:
        time.sleep(30)
        tick += 1
        try:
            if not TRACK.get("enabled"):
                continue
            if not TRACK.get("active"):
                if tick % 4 != 0:
                    continue
                with _state_lock:
                    has = any(p.get("type") == "photo"
                              for p in STATE.get("pending", []))
                if not has:
                    _request_verify_photo()
                continue
            last = TRACK.get("last_frame_ts", 0.0)
            if last and (time.time() - last) < 90:
                continue
            with _state_lock:
                already = any(p.get("type") == "tracking" and p.get("message") == "on"
                              for p in STATE.get("pending", []))
            if already:
                continue  # 等设备来领取，别刷队列
            if _push_pending_command("tracking", "on"):
                log("跟踪保活：超过 90s 未收到 track 帧，已补发开启指令", "WARN")
        except Exception as exc:
            log("跟踪保活异常: %s" % exc, "WARN")


# ---- 跟踪视频流通道（2026-10-17）----
# 此前跟踪帧逐帧 POST /vision，每帧重建 TCP 连接 + HTTP/multipart 开销把
# 帧率压在 2-3fps。改为设备在一条常连接上连续推帧：
#   协议：4 字节小端帧长 + JPEG 字节；应答 ASCII "pan,tilt\n"。
#   跟踪已关时回 "off\n"，设备收到即自停（省 pending 队列一来一回）。

_TRACK_STREAM_MAX = 500_000  # 帧长上限，防异常长度打爆内存


def _recv_exact(conn, n):
    """收满 n 字节；对端断开返回 None。"""
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _track_stream_client(conn, addr):
    conn.settimeout(3.0)
    try:
        # 关 Nagle（2026-10-23）：每帧应答只有 "pan,tilt\n" 十来个字节，
        # 默认 Nagle 会等 ACK 再发，小包往返平白多几十毫秒；跟踪闭环里
        # 每一帧都要付这笔账，是"一卡一卡"的隐性成本之一。
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            head = _recv_exact(conn, 4)
            if head is None:
                return
            (length,) = struct.unpack("<I", head)
            if length == 0 or length > _TRACK_STREAM_MAX:
                log(f"track流：{addr} 非法帧长度 {length}，断开", "WARN")
                return
            jpeg = _recv_exact(conn, length)
            if jpeg is None:
                return
            if not TRACK.get("enabled") or not TRACK.get("active"):
                # 关闭/待机/熔断回正期间都回 off：设备收到即自停，
                # 不再继续推帧（2026-10-21 修：此前只看 enabled，软熔断后
                # 设备继续跟，钉死计时连击导致 1 秒内误锁死）
                conn.sendall(b"off\n")
                continue
            _t0 = time.time()
            angle, tilt = tracking_compute_angle(jpeg)
            log(f"track流: {len(jpeg)//1024}KB 桥接处理{(time.time()-_t0)*1000:.0f}ms "
                f"-> pan={angle} tilt={tilt}")
            conn.sendall(("%d,%d\n" % (angle, tilt)).encode())
    except (OSError, socket.timeout) as exc:
        log(f"track流：{addr} 连接断开（{exc.__class__.__name__}）")
    finally:
        try:
            conn.close()
        except OSError:
            pass


def _track_stream_server():
    port = int(cfg("tracking", "stream_port", default=8789))
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(2)
    log(f"跟踪视频流端口：{port}")
    while True:
        try:
            conn, addr = srv.accept()
        except OSError:
            time.sleep(1)
            continue
        threading.Thread(target=_track_stream_client, args=(conn, addr),
                         daemon=True, name="track-stream").start()


@app.route("/servo", methods=["POST"])
def servo_set_angle():
    """直接设舵机角度（2026-10-16）：/servo?angle=N[&tilt=M] 或 JSON
    {"angle": N, "tilt": M}。经 pending 队列下发，msg = "h" 或 "h,v"。"""
    angle = request.values.get("angle", type=int)
    if angle is None:
        body = request.get_json(silent=True) or {}
        try:
            angle = int(body.get("angle"))
        except (TypeError, ValueError):
            angle = None
    tilt = request.values.get("tilt", type=int)
    if tilt is None:
        body = request.get_json(silent=True) or {}
        try:
            tilt = int(body.get("tilt"))
        except (TypeError, ValueError):
            tilt = None
    if angle is None or not 0 <= angle <= 180:
        return jsonify({"ok": False, "error": "angle must be 0-180"}), 400
    if tilt is not None and not 0 <= tilt <= 180:
        return jsonify({"ok": False, "error": "tilt must be 0-180"}), 400
    # ---- 安全限幅（2026-10-21）：手动指令也夹到软限位内，防误操作顶死舵机。
    # 标定行程时可在 config.yaml 设 tracking.servo_clamp=false 临时放开。----
    clamped = {}
    if cfg("tracking", "servo_clamp", default=True):
        tc = _track_cfg()
        ca = max(tc["min_angle"], min(tc["max_angle"], angle))
        if ca != angle:
            clamped["angle"] = [angle, ca]
            log(f"舵机安全限幅：水平 {angle}° → {ca}°（软限位 "
                f"{tc['min_angle']}~{tc['max_angle']}）", "WARN")
            angle = ca
        if tilt is not None:
            ct = max(tc["tilt_min_angle"], min(tc["tilt_max_angle"], tilt))
            if ct != tilt:
                clamped["tilt"] = [tilt, ct]
                log(f"舵机安全限幅：俯仰 {tilt}° → {ct}°（软限位 "
                    f"{tc['tilt_min_angle']}~{tc['tilt_max_angle']}）", "WARN")
                tilt = ct
    msg = str(angle) if tilt is None else f"{angle},{tilt}"
    ok = _push_pending_command("servo", msg)
    log(f"舵机：下发角度 {angle}°" +
        (f" 俯仰 {tilt}°" if tilt is not None else "") +
        ("" if ok else "（队列满）"))
    return jsonify({"ok": True, "angle": angle, "tilt": tilt,
                    "clamped": clamped or None, "queued": ok})


@app.route("/tracking/status", methods=["GET"])
def tracking_status():
    age = (time.time() - TRACK["last_face_ts"]) if TRACK["last_face_ts"] else None
    sc = _safety_cfg()

    def _pin(key):
        st = TRACK.get(key)
        if not st:
            return None
        return {"dir": st.get("dir"),
                "seconds": round(time.time() - st["since"], 1)}
    return jsonify({
        "enabled": TRACK["enabled"],
        "active": TRACK.get("active"),
        "angle": TRACK["angle"],
        "tilt": TRACK.get("tilt"),
        "frames": TRACK["frames"],
        "face_frames": TRACK["face_frames"],
        "face_age_s": round(age, 1) if age is not None else None,
        "start_ts": TRACK["start_ts"],
        # 安全保障状态（2026-10-21）
        "breaker": TRACK.get("breaker"),
        "trip_count": TRACK.get("trip_count", 0),
        "pin_pan": _pin("pin_pan"),
        "pin_tilt": _pin("pin_tilt"),
        "stall_limit_s": sc["stall_limit_s"],
        "servo_clamp": sc["servo_clamp"],
        "restart_cooldown_s": sc["restart_cooldown_s"],
        "trip_lockout": sc["trip_lockout"],
        "owner_sim": TRACK.get("owner_sim"),
        "follow_owner_only": sc["follow_owner_only"],
    })


@app.route("/api/dashboard/tracking", methods=["GET"])
def dashboard_tracking_get():
    """Dashboard「人脸跟踪」页：状态汇总（2026-10-23）"""
    tc = _track_cfg()
    sc = _safety_cfg()
    with _state_lock:
        has_override = bool(STATE.get("tracking_home"))
    return jsonify({
        "ok": True,
        "enabled": TRACK["enabled"],
        "active": TRACK.get("active"),
        "angle": TRACK["angle"],
        "tilt": TRACK.get("tilt"),
        "owner_sim": TRACK.get("owner_sim"),
        "breaker": TRACK.get("breaker"),
        "trip_count": TRACK.get("trip_count", 0),
        "home_pan": tc["home_pan"],
        "home_tilt": tc["home_tilt"],
        "home_source": "dashboard" if has_override else "config",
        "limits": {"pan": [tc["min_angle"], tc["max_angle"]],
                   "tilt": [tc["tilt_min_angle"], tc["tilt_max_angle"]]},
        "templates": 0 if _user_templates is None
        else int(len(_user_templates)),
        "follow_owner_only": sc["follow_owner_only"],
    })


@app.route("/api/dashboard/tracking", methods=["POST"])
def dashboard_tracking_save():
    """保存或预览回位角度（2026-10-23）。

    {"home_pan": 100, "home_tilt": 110, "preview": false}
    - preview=true：只让头转过去（跟踪没在跟时），不落盘
    - 否则：写 state.json 覆盖层，立即生效免重启
    """
    data = request.get_json(silent=True) or {}
    # 一键恢复 config.yaml 默认值（2026-10-23）：清掉 state.json 覆盖层
    if data.get("reset"):
        with _state_lock:
            STATE.pop("tracking_home", None)
            save_state(STATE)
        tc = _track_cfg()
        log("回位角度已恢复 config.yaml 默认：pan=%d tilt=%d"
            % (tc["home_pan"], tc["home_tilt"]))
        return jsonify({"ok": True, "reset": True,
                        "home_pan": tc["home_pan"],
                        "home_tilt": tc["home_tilt"], "saved": True})
    tc = _track_cfg()
    try:
        pan = int(data.get("home_pan", tc["home_pan"]))
        tilt = int(data.get("home_tilt", tc["home_tilt"]))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "角度必须是整数"}), 400
    # 必须严格落在软限位内且离边 ≥2°（启动自检同口径，防一开跟踪就顶死）
    p_lo, p_hi = tc["min_angle"] + 2, tc["max_angle"] - 2
    t_lo, t_hi = tc["tilt_min_angle"] + 2, tc["tilt_max_angle"] - 2
    if not p_lo <= pan <= p_hi:
        return jsonify({"ok": False,
                        "error": f"水平回位角需在 {p_lo}~{p_hi} 之间"}), 400
    if not t_lo <= tilt <= t_hi:
        return jsonify({"ok": False,
                        "error": f"俯仰回位角需在 {t_lo}~{t_hi} 之间"}), 400
    preview = bool(data.get("preview"))
    if not preview:
        with _state_lock:
            STATE["tracking_home"] = {"pan": pan, "tilt": tilt}
            save_state(STATE)
        log("回位角度已更新（Dashboard）：pan=%d tilt=%d" % (pan, tilt))
    moved = False
    if preview and not TRACK.get("active"):
        # 只在跟踪没在跟时预览转头（2026-10-23 用户定稿：跟踪期间滑杆
        # 只改数值不抢舵机——头该跟着人走，不该跟着滑杆走）
        moved = _push_servo_preview(pan, tilt)
    return jsonify({"ok": True, "home_pan": pan, "home_tilt": tilt,
                    "saved": not preview, "servo_preview": moved,
                    "tracking_active": bool(TRACK.get("active"))})


@app.route("/api/dashboard/tracking/enroll", methods=["POST"])
def dashboard_tracking_enroll():
    """Dashboard 直接注册主人脸（2026-10-23）：multipart file= 或原始 JPEG。
    ?replace=1 清空重注册。参考照落盘 data/owner_ref.jpg 供页面回显。"""
    file_obj = request.files.get("file")
    image_bytes = file_obj.read() if file_obj is not None else request.get_data()
    if not image_bytes:
        return jsonify({"ok": False, "error": "没有收到照片"}), 400
    replace = (str(request.form.get("replace", "")).lower()
               in ("1", "true", "yes")) or \
        (request.args.get("replace") in ("1", "true", "yes"))
    ok, info = _enroll_owner_face(image_bytes, replace=replace)
    if not ok:
        return jsonify({"ok": False, "error": str(info)}), 400
    try:
        OWNER_REF_FILE.parent.mkdir(parents=True, exist_ok=True)
        OWNER_REF_FILE.write_bytes(image_bytes)
    except OSError as exc:
        log("参考照落盘失败：%s" % exc, "WARN")
    log("Dashboard 注册主人脸：模板 %d 张%s"
        % (info["templates"], "（已清空重注册）" if replace else "（追加）"))
    return jsonify({"ok": True, "templates": info["templates"],
                    "self_sim": info["self_sim"], "replaced": replace,
                    "ref_url": "/api/dashboard/tracking/ref.jpg?t=%d"
                    % int(time.time())})


@app.route("/api/dashboard/tracking/ref.jpg", methods=["GET"])
def dashboard_tracking_ref():
    if not OWNER_REF_FILE.exists():
        return "", 404
    return send_file(str(OWNER_REF_FILE), mimetype="image/jpeg")


@app.route("/vision", methods=["POST"])
def vision():
    """
    兼容官方 StackChanCamera::Explain() 的 multipart/form-data 格式：
      字段 question (文本) + file (image/jpeg)
    返回一段人话，固件会把它交给云端 LLM。
    """
    if not check_token():
        return "unauthorized", 401

    question = request.form.get("question", "")
    file_obj = request.files.get("file")
    if file_obj is None:
        # 兼容直接 POST 原始 JPEG 的情况
        image_bytes = request.get_data()
    else:
        image_bytes = file_obj.read()

    if not image_bytes:
        return "没有收到图像数据。", 400

    device_id = (request.headers.get("Device-Id")
                 or request.headers.get("device-id") or "unknown")
    # 照片用途（2026-09-16 双拍摄位）：seat=在座判定 / cup=水杯水位判定
    photo_type = (request.headers.get("X-Photo-Type") or "seat").lower()

    # ---- 水杯照：不做在座判定，不进久坐计时，只做水位判定 ----
    if photo_type == "cup":
        now = datetime.now().isoformat(timespec="seconds")
        photo_name = _save_photo(image_bytes, device_id) \
            if cfg("notify", "save_photos", default=False) else None
        _append_analysis({
            "type": "cup_photo",
            "ts": now,
            "file": photo_name,
            "device": device_id,
        })
        log(f"vision(cup): 收到水杯照（{len(image_bytes)}B，file={photo_name}）")
        # 水位判定异步跑；在座门闩用最近一次 seat 照的判定结果
        with _state_lock:
            last_presence = STATE.get("last_presence")
        if _effective_water_enabled():
            threading.Thread(
                target=handle_water_check, args=(image_bytes, last_presence),
                kwargs={"photo_ts": now, "photo_name": photo_name},
                daemon=True).start()
        return "好的，我看看水杯。"

    # ---- 跟踪帧（2026-10-14 舵机云台）：只算角度回传，不落盘、不进在座状态机 ----
    if photo_type == "track":
        if not TRACK.get("enabled"):
            return jsonify({"ok": False, "reason": "tracking_off"}), 200
        _t0 = time.time()
        angle, tilt = tracking_compute_angle(image_bytes)
        # 2026-10-16 平滑化调优：打印每帧耗时与图大小，定位瓶颈（稳定后删）
        log(f"track帧: {len(image_bytes)//1024}KB 桥接处理{(time.time()-_t0)*1000:.0f}ms "
            f"-> pan={angle} tilt={tilt}")
        return jsonify({"ok": True, "servo": angle, "tilt": tilt}), 200

    # ---- 扩展拍摄位照（2026-09-17 多拍摄位）：ex1/ex2/ex3，只存档不做判定 ----
    if photo_type.startswith("ex") and photo_type[2:].isdigit():
        idx = int(photo_type[2:]) - 1
        extras = (_photo_positions().get("extras") or [])
        name = extras[idx]["name"] if 0 <= idx < len(extras) else photo_type
        now = datetime.now().isoformat(timespec="seconds")
        photo_name = _save_photo(image_bytes, device_id)
        _append_analysis({
            "type": "extra_photo",
            "ts": now,
            "file": photo_name,
            "name": name,
            "source": photo_type,
            "device": device_id,
        })
        log(f"vision({photo_type}): 收到扩展位「{name}」记录照"
            f"（{len(image_bytes)}B，file={photo_name}）")
        return "好的，拍好了。"

    # 2026-09-16：判定与状态机全部移到异步线程（原实现把云端 API 调用锁在
    # _state_lock 里同步执行，秒级延迟既阻塞设备 HTTP 响应又锁死全局状态）。
    # 快速路径只做采样计数 + 照片落盘，立即应答设备；见 handle_seat_check。
    with _state_lock:
        reset_daily_if_needed(STATE)
        STATE["samples_today"] = STATE.get("samples_today", 0) + 1

    photo_name = None
    if cfg("notify", "save_photos", default=False):
        photo_name = _save_photo(image_bytes, device_id)
    photo_ts = datetime.now().isoformat(timespec="seconds")
    threading.Thread(
        target=handle_seat_check,
        args=(image_bytes, question, device_id, photo_name, photo_ts),
        daemon=True).start()
    return "好的，我看一眼。", 200


def handle_seat_check(image_bytes, question, device_id, photo_name, photo_ts):
    """在座判定 + 久坐状态机（异步线程）。云 API 判定秒级延迟，不阻塞 /vision。"""
    presence, detail = judge_presence(image_bytes, question)
    note_presence(presence)

    now = datetime.now()
    with _state_lock:
        STATE["last_vision_ts"] = now.isoformat(timespec="seconds")
        STATE["last_presence"] = presence
        # 2026-10-27 修复「离开时长算少」：视觉确认离座（无脸+无动作）就
        # 记为离座起点，不再等设备哨兵的"离座"事件。此前 16:14 人已走、
        # 视觉连续判 absent，但哨兵 16:38 才报离座 → 回归只播"约 4 分钟"
        # （真实 28 分钟）。起点只在本轮首次确立，重复事件不覆盖。
        if (presence == "absent"
                and isinstance(detail, dict)
                and detail.get("reason") == "no_face_no_motion_confirmed"
                and not STATE.get("in_seat_since")
                and not STATE.get("away_since")):
            STATE["away_since"] = now.isoformat(timespec="seconds")
            log("视觉确认离座，开始记录离座起点"
                f"（{STATE['away_since']}）")
        STATE["last_vision_faces_large"] = (
            detail.get("faces_large", 0) if isinstance(detail, dict) else 0)
        # 人脸识别（2026-10-10）：对账/验明正身改认"主人脸"
        STATE["last_vision_user_faces"] = (
            detail.get("user_faces", 0) if isinstance(detail, dict) else 0)
        STATE["last_vision_user_sim"] = (
            detail.get("user_sim") if isinstance(detail, dict) else None)
        STATE["last_photo_meta"] = {
            "device": device_id,
            "bytes": len(image_bytes),
            "detail": detail,
            "ts": STATE["last_vision_ts"],
        }

        absent_streak_reset = cfg("presence", "absent_streak_to_reset",
                                  default=2)

        if presence == "in_seat":
            STATE["absent_streak"] = 0
            now_iso = now.isoformat(timespec="seconds")
            if not STATE.get("in_seat_since"):
                STATE["in_seat_since"] = now_iso
                log("检测到有人入座，开始计时")
                # 视觉兜底回归播报（2026-10-10）：幻影回归被拦截后，设备侧
                # 哨兵状态已切成"在座"，用户真回来时设备不再发"回归"事件，
                # 正常的哨兵播报路径就不会触发（当日 13:28 实测漏播）。
                # 这里由视觉轮询兜底：此前有确认过的真离座（away_since 在），
                # 现在照片见到主人脸入座 → 补一条回归播报，按 away_since
                # 补算时长。两个防打架守卫：
                #   - pending_return 在 → 交给 _check_pending_return 验明
                #     正身路径播报（它有更准的事件时间，避免双播）；
                #   - pending_away 在 → 离座还在复核中，见脸属于"误报离座"，
                #     由复核路径静默恢复计时，不能播"回来了"。
                away_since = _parse_ts(STATE.get("away_since") or "")
                if (away_since and not STATE.get("pending_return")
                        and not STATE.get("pending_away")):
                    minutes = max(
                        1, int((now - away_since).total_seconds() / 60))
                    msg = f"主人回到座位啦，刚才离开了大约 {minutes} 分钟。"
                    STATE["away_since"] = None
                    STATE["last_remind_ts"] = None
                    push_speak(STATE, msg, source="sentinel")
                    log(f"视觉兜底回归播报：确认离座 {minutes} 分钟后"
                        f"照片见主人脸入座（{msg}）")
                # 打卡式记录（P2-⑭）：今天第一次确认在座 → 推飞书
                # 打卡时间无条件记录（进状态与下班汇总），飞书推送才受开关控制
                if (cfg("punch_in", "enabled", default=True)
                        and STATE.get("first_in_seat_ts") is None):
                    STATE["first_in_seat_ts"] = now_iso
                    if cfg("notify", "feishu", default=True):
                        threading.Thread(
                            target=notify_feishu,
                            args=(f"📍 今日 {now.strftime('%H:%M')} 到工位",),
                            daemon=True).start()
            STATE["last_in_seat_ts"] = now_iso

            # 访客提醒（P2-⑬，2026-10-10 人脸识别版）：座位区域里出现
            # "不是主人"的大脸 → 飞书记录。识别未启用时退回旧逻辑
            # （第二张大脸）。远处邻座的小脸不算。
            strangers = (detail.get("stranger_faces") if isinstance(detail, dict)
                         else None)
            recog_on = (detail.get("user_sim") is not None
                        or (isinstance(detail, dict)
                            and detail.get("user_faces", 0) > 0))
            if recog_on and strangers is not None:
                faces = strangers
                min_faces = 1
            else:
                faces = (detail.get("faces_large") if isinstance(detail, dict)
                         else None)
                if faces is None:
                    faces = (detail.get("faces", 0)
                             if isinstance(detail, dict) else 0)
                min_faces = int(cfg("visitor", "min_faces", default=2))
            if cfg("visitor", "enabled", default=True) and faces >= min_faces:
                v_last = _parse_ts(STATE.get("visitor_last_ts"))
                cooldown = float(cfg("visitor", "cooldown_minutes", default=10))
                if v_last is None or (now - v_last).total_seconds() >= cooldown * 60:
                    STATE["visitor_last_ts"] = now_iso
                    log(f"访客提醒：画面中出现 {faces} 张非主人脸"
                        if recog_on else f"访客提醒：画面中出现 {faces} 张脸")
                    if cfg("notify", "feishu", default=True):
                        threading.Thread(
                            target=notify_feishu,
                            args=(f"👀 工位提醒：{now.strftime('%H:%M')} 画面里出现了 "
                                  f"{faces} 个人，可能有人来找你。",),
                            daemon=True).start()
                    if cfg("visitor", "speak", default=True):
                        push_speak(STATE, "主人，有人来到你的工位附近了。",
                                   source="visitor")

            # 专注度（P2-⑫）：ollama 模式下模型顺带判断"是否在玩手机"
            if (isinstance(detail, dict) and detail.get("focus") == "distracted"
                    and cfg("focus", "enabled", default=False)):
                f_last = _parse_ts(STATE.get("focus_last_ts"))
                f_cd = float(cfg("focus", "cooldown_minutes", default=45))
                if f_last is None or (now - f_last).total_seconds() >= f_cd * 60:
                    STATE["focus_last_ts"] = now_iso
                    log("专注度提醒：模型判断当前分心（玩手机等）")
                    if cfg("notify", "feishu", default=True):
                        threading.Thread(
                            target=notify_feishu,
                            args=("🧐 专注度提醒：最近一张画面里看起来没有在专心工作"
                                  "（可能在看手机）。",),
                            daemon=True).start()

            # 坐姿提醒（2026-09-16 行为分析立项，用户：坐姿是否端正/有没有驼背）：
            # 连续 N 轮（默认 2 轮 ≈ 10 分钟）驼背/趴桌 → 提醒一次；
            # 坐姿恢复 GOOD 清零计数。云端判定失败/旧格式无 posture 时静默跳过。
            posture = detail.get("posture") if isinstance(detail, dict) else None
            if cfg("behavior", "posture_enabled", default=True):
                STATE["posture_last"] = posture
                if posture in ("hunching", "slumped"):
                    STATE["posture_consecutive_hunch"] = (
                        STATE.get("posture_consecutive_hunch", 0) + 1)
                    streak = STATE["posture_consecutive_hunch"]
                    need = int(cfg("behavior", "posture_consecutive_rounds",
                                   default=2))
                    p_last = _parse_ts(STATE.get("posture_last_remind_ts"))
                    p_cd = float(cfg("behavior", "posture_cooldown_minutes",
                                     default=60))
                    p_limit = int(cfg("behavior", "posture_daily_limit",
                                      default=4))
                    if (streak >= need
                            and (p_last is None
                                 or (now - p_last).total_seconds() >= p_cd * 60)
                            and STATE.get("posture_reminds_today", 0) < p_limit):
                        STATE["posture_last_remind_ts"] = now_iso
                        STATE["posture_reminds_today"] = (
                            STATE.get("posture_reminds_today", 0) + 1)
                        STATE["posture_consecutive_hunch"] = 0
                        log(f"坐姿提醒：连续 {streak} 轮 {posture}")
                        if cfg("notify", "feishu", default=True):
                            threading.Thread(
                                target=notify_feishu,
                                args=("🪑 坐姿提醒：最近两轮画面里坐姿明显不佳"
                                      "（驼背/趴桌），注意腰背。",),
                                daemon=True).start()
                        if cfg("behavior", "speak", default=True):
                            push_speak(
                                STATE,
                                cfg("behavior", "posture_message",
                                    default="主人，坐直一点，别驼背哦。"),
                                source="posture")
                elif posture == "good":
                    STATE["posture_consecutive_hunch"] = 0
        elif presence == "absent":
            STATE["absent_streak"] = STATE.get("absent_streak", 0) + 1
            if STATE["absent_streak"] >= absent_streak_reset:
                if STATE.get("in_seat_since"):
                    was = STATE["in_seat_since"]
                    seg = (now - (_parse_ts(was) or now)).total_seconds()
                    STATE["in_seat_seconds_today"] = (
                        STATE.get("in_seat_seconds_today", 0) + int(seg))
                    mins = seg / 60
                    log(f"连续 {STATE['absent_streak']} 次无人，"
                        f"在座计时归零（上一轮约 {int(mins)} 分钟）")
                STATE["in_seat_since"] = None
                STATE["last_remind_ts"] = None
        # unknown 不改变计时状态

        # 离座访客提醒（2026-10-10 人脸识别解锁）：主人不在工位时，
        # 座位区域出现"非主人"的大脸 → 飞书推送（识别开启才有 stranger_faces）
        if (cfg("visitor", "enabled", default=True)
                and cfg("visitor", "alert_when_away", default=True)
                and STATE.get("in_seat_since") is None
                and isinstance(detail, dict)
                and int(detail.get("stranger_faces") or 0) >= 1):
            av_last = _parse_ts(STATE.get("away_visitor_last_ts"))
            av_cd = float(cfg("visitor", "cooldown_minutes", default=10))
            if av_last is None or (now - av_last).total_seconds() >= av_cd * 60:
                STATE["away_visitor_last_ts"] = now.isoformat(timespec="seconds")
                log("离座访客：主人不在时座位区域出现陌生人")
                if cfg("notify", "feishu", default=True):
                    threading.Thread(
                        target=notify_feishu,
                        args=(f"👀 离座访客：{now.strftime('%H:%M')} 你不在工位时，"
                              "座位区域出现了别人（可能是来找你的或路过）。",),
                        daemon=True).start()
                # 2026-10-15 用户：离座时来人才需要提醒（在座自己看得到）。
                # 飞书可能没开，语音播报是主通知路径。
                if cfg("visitor", "speak_when_away", default=True):
                    push_speak(STATE,
                               "主人，你不在工位的时候，有人来到你的座位附近了。",
                               source="visitor_away")

        since = _parse_ts(STATE.get("in_seat_since"))
        minutes_in_seat = (now - since).total_seconds() / 60 if since else None

        msg = evaluate_reminder(STATE, presence)
        if msg:
            push_reminder(STATE, msg)

        # 照片已在上传线程内落盘（photo_name 由参数传入，这里不再重复保存）

        # 分析流水（Dashboard 用）：拍照判定结果 + 照片关联
        _append_analysis({
            "type": "vision",
            "ts": STATE["last_vision_ts"],
            "file": photo_name,
            "presence": presence,
            "photo_type": "seat",
            "detail": detail if isinstance(detail, dict) else {"raw": str(detail)},
            "device": device_id,
        })

        save_state(STATE)

    # 喝水判定只属于水杯照（cup 分支已异步触发）；这里的老残留（seat 照也触发
    # 水位判定）已删除——2026-09-16 双拍摄位分流后不再需要。
    log(f"vision: presence={presence} detail={detail} "
        f"in_seat_min={None if minutes_in_seat is None else round(minutes_in_seat)}")

    # 跟踪自动重启（2026-10-16 用户方案）：跟踪开着但处于待机（丢脸超时
    # 已停）时，在座判定看到主人在座 → 自动重启跟踪。
    # 2026-10-21 配套熔断器：熔断回正后同样走这里恢复；60s 冷却给瞬态
    # 原因（人站在视野边缘等）消散的时间，防"重启即熔断"连击。
    if TRACK.get("enabled") and not TRACK.get("active") and presence == "in_seat":
        sc = _safety_cfg()
        since_trip = time.time() - (TRACK.get("last_trip_ts") or 0.0)
        if TRACK.get("last_trip_ts") and since_trip < sc["restart_cooldown_s"]:
            log("舵机熔断后冷却中（还剩 %.0fs），暂缓自动重启跟踪"
                % (sc["restart_cooldown_s"] - since_trip), "WARN")
        else:
            tc = _track_cfg()
            TRACK["active"] = True
            TRACK["angle"] = tc["home_pan"]
            TRACK["tilt"] = tc["home_tilt"]
            TRACK["last_face_ts"] = 0.0
            TRACK["last_frame_ts"] = time.time()
            _push_pending_command("tracking", "on")
            log("人脸跟踪：在座判定看到主人回来，自动重启跟踪")

    # 哨兵"回归"验明正身：暂扣中的回归事件用本张判定照片复核
    _check_pending_return(presence, detail)


def _save_photo(image_bytes, device_id):
    """落盘一张照片，返回文件名（失败返回 None）。"""
    try:
        if not PHOTO_DIR.exists():
            PHOTO_DIR.mkdir(parents=True, exist_ok=True)
        safe_dev = re.sub(r"[^A-Za-z0-9]", "", device_id)[:16] or "dev"
        name = PHOTO_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{safe_dev}.jpg"
        with open(name, "wb") as f:
            f.write(image_bytes)
        _cleanup_photos()
        return name.name
    except OSError as e:
        log(f"保存照片失败：{e}", "WARN")
        return None


def _append_analysis(record):
    """往 analysis.jsonl 追加一行（Dashboard 分析时间线的数据源）。"""
    try:
        with _ANALYSIS_LOCK:
            with open(ANALYSIS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        log(f"分析流水写入失败：{e}", "WARN")


def _read_analysis_tail(limit=30):
    """读 analysis.jsonl 末尾 N 行（新的在后）。"""
    if not ANALYSIS_FILE.exists():
        return []
    out = []
    try:
        with _ANALYSIS_LOCK:
            with open(ANALYSIS_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
        for raw in lines[-limit:]:
            raw = raw.strip()
            if not raw:
                continue
            try:
                out.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    except OSError:
        pass
    return out


def _cleanup_photos():
    days = cfg("notify", "photo_retention_days", default=3)
    cutoff = time.time() - days * 86400
    for p in PHOTO_DIR.glob("*.jpg"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
        except OSError:
            pass


def _keyword_allowed(command):
    allow = cfg("security", "task_keywords", default=[])
    if not allow:
        return True
    # "*" = 放行一切（P1-⑧：不想被白名单拦时的总开关）
    if "*" in allow:
        return True
    return any(k in command for k in allow)


def _extract_conclusion(out, limit=120):
    """从 Hermes 输出末尾提取一行结论（prompt 要求它最后写一行 ≤80 字的播报词）。

    2026-09-20：limit 60→120（产品决策方案 A），贴着 TTS 120 字上限，
    让机器人能说得更完整；prompt 侧同步放宽到 80 字以内。

    倒着找第一条"像人话"的行：跳过空行、JSON、markdown 分隔线。
    找不到就返回空串，由调用方用兜底话术。
    """
    for raw in reversed((out or "").splitlines()):
        line = raw.strip().strip('"').strip()
        if not line:
            continue
        if line.startswith(("{", "[", "```", "---", "#")):
            continue
        if len(line) < 4:
            continue
        return line[:limit]
    return ""


def _notify_robot_speak(message, source="hermes_task"):
    """任务完成后让机器人开口（在本线程内直接入队，不走 HTTP）。

    必须拿 _state_lock：run_hermes_task 在后台线程跑，pending/speaks_today
    与 HTTP 线程共享 STATE。
    """
    if not cfg("hermes", "notify_completion", default=True):
        return
    with _state_lock:
        reset_daily_if_needed(STATE)
        # 任务回执不占每日播报额度（2026-09-15：额度把回执吞了，用户以为固件有 bug）
        ok = push_speak(STATE, message, source=source, count_daily=False)
        save_state(STATE)
    if not ok:
        log("任务完成播报未能入队（队列满或达每日上限）", "WARN")


def run_hermes_task(command, source="stackchan"):
    """后台线程里执行 Hermes 任务"""
    bin_path = cfg("hermes", "bin", default="hermes")
    extra = cfg("hermes", "extra_args", default=[]) or []
    timeout = cfg("hermes", "task_timeout", default=600)

    # 播报时要提"你刚才让我做什么"，超长就截短
    brief = command if len(command) <= 30 else command[:30] + "……"

    prompt = (
        f"【来自 StackChan 桌面机器人的语音任务】\n"
        f"{command}\n\n"
        f"要求：\n"
        f"1. 使用你已配置的飞书技能（feishu-lark-cli 等）完成这件事。\n"
        f"2. 完成后把结果整理成简洁的中文，通过 hermes send 推送到飞书。\n"
        f"3. 不要把长文内容念出来，只推送。\n"
        f"4. 在最后一行输出一行简短结论，供机器人播报（80 字以内）。"
    )

    args = [bin_path, "-z", prompt] + list(extra)
    log(f"派发 Hermes 任务：{command[:80]}")
    try:
        proc = subprocess.run(args, capture_output=True, text=True,
                              timeout=timeout, cwd=str(BASE_DIR))
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        if proc.returncode == 0:
            log(f"Hermes 任务完成（{len(out)} 字符输出）")
            if err:
                log(f"Hermes stderr: {err[:300]}", "WARN")

            # ---- 回执链路（2026-09-14 补上）：结果进飞书后让机器人开口 ----
            # prompt 第 4 条要求 Hermes 在最后一行写 ≤30 字结论，之前没人消费它。
            conclusion = _extract_conclusion(out)
            if conclusion:
                _notify_robot_speak(f"任务完成了。{conclusion}", source=source)
            else:
                _notify_robot_speak(
                    f"任务完成了。「{brief}」的结果已经发到你的飞书，记得看一下。",
                    source=source)
        else:
            log(f"Hermes 任务失败 rc={proc.returncode}\nstdout={out[:500]}"
                f"\nstderr={err[:500]}", "ERROR")
            _notify_robot_speak(
                f"刚才那个任务没做成。「{brief}」失败了，等会儿可以再说一遍让我重试。",
                source=source)
    except subprocess.TimeoutExpired:
        log(f"Hermes 任务超时（>{timeout}s）", "ERROR")
        _notify_robot_speak(
            f"那个任务跑了太久被我掐了。「{brief}」没做完，电脑可能忙，稍后再试。",
            source=source)
    except OSError as e:
        log(f"Hermes 任务启动失败：{e}", "ERROR")
        _notify_robot_speak("任务没能启动，电脑上的 Hermes 服务好像没就绪。", source=source)


@app.route("/task", methods=["POST"])
def task():
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or request.form.to_dict() or {}
    command = (data.get("command") or data.get("text") or "").strip()
    source = data.get("source", "stackchan")
    if not command:
        return jsonify({"ok": False, "error": "empty command"}), 400

    if not _keyword_allowed(command):
        log(f"任务被白名单拦截：{command[:80]}", "WARN")
        return jsonify({"ok": False,
                        "error": "命令不在允许范围内",
                        "message": "这个我暂时做不了，换个说法试试。"}), 200

    threading.Thread(target=run_hermes_task, args=(command, source),
                     daemon=True).start()
    return jsonify({
        "ok": True,
        "message": "好的，已经交给 Hermes 处理，结果稍后推送到你的飞书。",
        "command": command,
    })


# ==========================================================================
#  TTS 语音合成（2026-09-14）
#  小智云端实测不认 WakeWordInvoke 注入的文字（只当误唤醒，进聆听等 65s 超时），
#  云端 TTS 播报路线废弃。新路线：桥接用 edge-tts（免费）合成语音 → 转
#  OGG opus 24kHz mono（匹配设备解码管线）→ 机器人 GET /tts/synth 下载 →
#  AudioService::PlaySound 本地喇叭直接播。同文本+音色做磁盘缓存。
# ==========================================================================

# ---- TTS 失败留痕告警（2026-10-21）----
# 教训：2026-10-12 起 edge-tts 403 静默失败 5 天（17 次播报无声）无人知晓。
# 失败必须计数并告警；30 分钟最多提醒一次防刷屏。
_tts_fail_stat = {"count": 0, "last_ts": 0.0, "last_alert_ts": 0.0}
_TTS_FAIL_ALERT_COOLDOWN_S = 1800.0


def _tts_fail_alert(err):
    _tts_fail_stat["count"] += 1
    _tts_fail_stat["last_ts"] = time.time()
    now = time.time()
    if now - _tts_fail_stat["last_alert_ts"] < _TTS_FAIL_ALERT_COOLDOWN_S:
        return
    _tts_fail_stat["last_alert_ts"] = now
    log("TTS 连续失败（累计 %d 次）：%s" % (_tts_fail_stat["count"], err),
        "ERROR")
    if cfg("notify", "feishu", default=True):
        threading.Thread(target=notify_feishu, args=(
            "🔊 小栗 TTS 告警\n语音合成失败（累计 %d 次），最新错误：%s\n"
            "播报会退化为提示音，请检查网络或 edge-tts 版本（需 >=7.3.1）。"
            % (_tts_fail_stat["count"], str(err)[:200]),),
            daemon=True).start()


def _tts_synthesize(text, voice, out_path):
    """edge-tts 合成 mp3 → PyAV 转 ogg opus 24kHz mono。成功返回 True。"""
    import tempfile
    from pathlib import Path as _Path

    try:
        import edge_tts
        import av as pyav
    except ImportError as e:
        log(f"TTS 依赖缺失（pip install edge-tts av）：{e}", "WARN")
        _tts_fail_alert(e)
        return False

    mp3_path = _Path(tempfile.gettempdir()) / f"stackchan_tts_{os.getpid()}.mp3"
    try:
        async def _synth():
            comm = edge_tts.Communicate(text, voice)
            await comm.save(str(mp3_path))

        asyncio.run(_synth())
        if not mp3_path.exists() or mp3_path.stat().st_size < 512:
            log("TTS 合成结果为空", "WARN")
            _tts_fail_alert("合成结果为空")
            return False

        inp = pyav.open(str(mp3_path))
        out = pyav.open(str(out_path), "w")
        ostream = out.add_stream("libopus", rate=24000)
        ostream.layout = "mono"
        # 关键：固件 PlaySound 按 60ms 帧重建解码器（与云端音频一致），
        # PyAV 默认 20ms 帧会导致设备端解码无声，必须对齐
        ostream.options = {"frame_duration": "60", "application": "audio"}
        for frame in inp.decode(audio=0):
            frame.pts = None
            for pkt in ostream.encode(frame):
                out.mux(pkt)
        out.close()
        inp.close()
        return True
    except Exception as e:  # noqa: BLE001
        log(f"TTS 合成失败：{e}", "WARN")
        _tts_fail_alert(e)
        return False
    finally:
        try:
            mp3_path.unlink(missing_ok=True)
        except Exception:
            pass


def _tts_cleanup(keep=200):
    """缓存超限就删最旧的（防止 data/tts 无限膨胀）"""
    try:
        files = sorted(TTS_DIR.glob("*.ogg"), key=lambda p: p.stat().st_mtime)
        for p in files[:-keep] if len(files) > keep else []:
            p.unlink(missing_ok=True)
    except Exception:
        pass


@app.route("/tts/synth", methods=["GET"])
def tts_synth():
    """机器人取播报语音：GET /tts/synth?text=...（URL 编码的 UTF-8）

    首次请求现场合成（2~5s），之后同文本命中磁盘缓存直接回。
    失败返回 5xx，固件收到非 200 就回退为本地提示音。
    """
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    if not cfg("tts", "enabled", default=True):
        return jsonify({"ok": False, "error": "tts disabled"}), 503

    text = (request.args.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "empty text"}), 400
    # 语音别太长：120 字约 25~30 秒音频，设备 PSRAM 和合成耗时都可控
    text = text[:120]

    voice = cfg("tts", "voice", default="zh-CN-XiaoxiaoNeural")
    key = hashlib.sha1(f"{voice}:{text}".encode("utf-8")).hexdigest()[:16]
    out_path = TTS_DIR / f"{key}.ogg"

    if not (out_path.exists() and out_path.stat().st_size > 512):
        TTS_DIR.mkdir(parents=True, exist_ok=True)
        if not _tts_synthesize(text, voice, out_path):
            return jsonify({"ok": False, "error": "tts failed"}), 503
        _tts_cleanup()
        log(f"TTS 合成：{out_path.name}（{out_path.stat().st_size}B，{voice}）")

    return send_file(str(out_path), mimetype="audio/ogg")


@app.route("/tts/url", methods=["GET"])
def tts_url():
    """机器人取播报语音的 URL（v1.9.8 notify 流式播报路线）：
    GET /tts/url?text=...（URL 编码的 UTF-8）→ {"ok": true, "url": "http://<host>/tts/audio/<key>.ogg"}

    与 /tts/synth 共用同一份磁盘缓存，只是不把音频塞进响应体，
    改为给设备一个 URL，由设备端 notify 引擎（PR #2191 移植）流式拉取。
    设备只要 URL，永远不在设备内存里攒整个文件。
    """
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    if not cfg("tts", "enabled", default=True):
        return jsonify({"ok": False, "error": "tts disabled"}), 503

    text = (request.args.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "empty text"}), 400
    text = text[:120]

    voice = cfg("tts", "voice", default="zh-CN-XiaoxiaoNeural")
    key = hashlib.sha1(f"{voice}:{text}".encode("utf-8")).hexdigest()[:16]
    out_path = TTS_DIR / f"{key}.ogg"

    if not (out_path.exists() and out_path.stat().st_size > 512):
        TTS_DIR.mkdir(parents=True, exist_ok=True)
        if not _tts_synthesize(text, voice, out_path):
            return jsonify({"ok": False, "error": "tts failed"}), 503
        _tts_cleanup()
        log(f"TTS 合成：{out_path.name}（{out_path.stat().st_size}B，{voice}）")

    # 用请求的 Host 回拼 URL：设备连的是哪个 IP，就回哪个 IP（免配置 NAT/多网卡问题）
    return jsonify({"ok": True, "key": key, "url": f"http://{request.host}/tts/audio/{key}.ogg"})


@app.route("/tts/audio/<key>", methods=["GET"])
def tts_audio(key):
    """notify 引擎的音频流端点：GET /tts/audio/<key>.ogg

    注意：此端点不带 token——设备端 notify 引擎发的是裸 GET，无法附带鉴权头。
    桥接只监听局域网（0.0.0.0:8787），key 为 16 位 sha1 硬校验，防路径穿越。
    """
    if not re.fullmatch(r"[0-9a-f]{16}\.ogg", key or ""):
        return jsonify({"ok": False, "error": "bad key"}), 400

    out_path = TTS_DIR / key
    if not (out_path.exists() and out_path.stat().st_size > 512):
        return jsonify({"ok": False, "error": "not found"}), 404

    resp = send_file(str(out_path), mimetype="audio/ogg", conditional=True)
    # 明确禁用压缩/分块编码歧义，设备端按 Content-Length 或 chunked 都能处理
    resp.headers["Accept-Ranges"] = "none"
    return resp


def _vision_recently_sees_user(max_age_s=390):
    """最近一次视觉判定是否为"在座且认出主人脸"，且证据未过时。

    拍照节奏约 5 分钟一张，390 秒（6.5 分钟）内算新鲜。人脸识别开启时
    user_faces>=1 才算真看到"主人"——纯 motion 兜底在座不可靠，陌生人脸
    更不能顶替主人。
    """
    with _state_lock:
        ts = STATE.get("last_vision_ts")
        pres = STATE.get("last_presence")
        user_faces = STATE.get("last_vision_user_faces")
        large = STATE.get("last_vision_faces_large", 0)
    if not ts or pres != "in_seat":
        return False
    t = _parse_ts(ts)
    if t is None or (datetime.now() - t).total_seconds() > max_age_s:
        return False
    if user_faces is not None:
        try:
            return int(user_faces or 0) >= 1
        except (TypeError, ValueError):
            return False
    # 识别未启用时退回旧判据：大脸才算
    try:
        return int(large or 0) >= 1
    except (TypeError, ValueError):
        return False


def _request_verify_photo():
    """给设备派一张"立即拍照复核"单（固件 photo 指令，2026-10-10）。

    哨兵离开/回归事件后不再干等下一个 5 分钟周期照：塞一张拍照单进
    pending 队列，设备 10 秒内领单立即拍照上传，验证延迟缩到 ~20 秒。
    队列里已有拍照单时不重复派。
    """
    with _state_lock:
        for it in STATE.get("pending", []):
            if isinstance(it, dict) and it.get("type") == "photo":
                return
        STATE["pending"].append({
            "type": "photo",
            "message": "verify",
            "source": "sentinel",
            "created_at": datetime.now().isoformat(timespec="seconds"),
        })
        save_state(STATE)
    log("已派单：设备立即拍照复核哨兵事件（~10s 内领单）")


def _check_pending_return(presence, detail):
    """哨兵"离开/回归"事件的验明正身（2026-10-10 派单拍照版）。

    哨兵事件到达时桥接先派单让设备立即拍照（_request_verify_photo），
    本函数在每张判定照片到达后复核暂扣中的事件：
      - 回归 + 见大脸 → 真回归：按 away_since 补算真实时长播报，计时重起；
      - 回归 + 无人脸 → 继续暂扣，720s 超时判幻影拦截；
      - 离座 + 见大脸 → 误报离座：恢复久坐计时（在座秒数未入账）；
      - 离座 + 无人脸 → 真离座：入账在座秒数。
    """
    with _state_lock:
        pr = STATE.get("pending_return")
        pa = STATE.get("pending_away")
        if not pr and not pa:
            return
        now = datetime.now()
        # 验明正身改认"主人脸"（2026-10-10 人脸识别）：识别开启时
        # user_faces>=1 才是真主人；未启用退回 faces_large
        user_faces = detail.get("user_faces") if isinstance(detail, dict) else None
        large = detail.get("faces_large", 0) if isinstance(detail, dict) else 0
        if user_faces is not None:
            seen = (presence == "in_seat" and int(user_faces or 0) >= 1)
        else:
            seen = (presence == "in_seat" and int(large or 0) >= 1)

        if pr:
            if seen:
                away_since = _parse_ts(STATE.get("away_since") or "")
                if away_since:
                    minutes = max(1, int((now - away_since).total_seconds() / 60))
                    msg = f"主人回到座位啦，刚才离开了大约 {minutes} 分钟。"
                else:
                    msg = pr.get("message") or "主人回到座位啦。"
                STATE["pending_return"] = None
                STATE["pending_away"] = None
                STATE["away_since"] = None
                STATE["in_seat_since"] = now.isoformat(timespec="seconds")
                STATE["last_remind_ts"] = None
                push_speak(STATE, msg, source="sentinel")
                save_state(STATE)
                log(f"哨兵回归验明正身：照片见到人脸 → 播报回归（{msg}）")
            else:
                ts = _parse_ts(pr.get("ts") or "")
                if ts and (now - ts).total_seconds() > 720:
                    STATE["pending_return"] = None
                    save_state(STATE)
                    log("哨兵回归判幻影：复核期内照片始终未见人脸，拦截播报")
                else:
                    log("哨兵回归暂扣中：本张照片未见人脸，继续等待")
            return

        # 离座复核
        away_ts = _parse_ts(pa.get("ts") or "") or now
        if seen:
            # 误报离座：人还在——在座秒数尚未入账，恢复计时即可
            STATE["pending_away"] = None
            STATE["away_since"] = None
            STATE["in_seat_since"] = away_ts.isoformat(timespec="seconds")
            save_state(STATE)
            log("哨兵离座验照：照片见人脸 → 判误报，久坐计时恢复（视为未离座）")
        else:
            seg = int(pa.get("seg") or 0)
            STATE["in_seat_seconds_today"] = (
                STATE.get("in_seat_seconds_today", 0) + seg)
            STATE["pending_away"] = None
            save_state(STATE)
            log("哨兵离座验照：照片无人脸 → 确认真离座，计时入账")


def _sweep_verify_timeouts():
    """设备离线等导致复核照迟迟不来时的惰性超时结算（/pending 轮询触发）。"""
    with _state_lock:
        now = datetime.now()
        changed = False
        pr = STATE.get("pending_return")
        if pr:
            ts = _parse_ts(pr.get("ts") or "")
            if ts and (now - ts).total_seconds() > 720:
                STATE["pending_return"] = None
                changed = True
                log("哨兵回归超时未获复核照，判幻影拦截")
        pa = STATE.get("pending_away")
        if pa:
            ts = _parse_ts(pa.get("ts") or "")
            if ts and (now - ts).total_seconds() > 720:
                STATE["in_seat_seconds_today"] = (
                    STATE.get("in_seat_seconds_today", 0)
                    + int(pa.get("seg") or 0))
                STATE["pending_away"] = None
                changed = True
                log("哨兵离座超时未获复核照，按真离座结算计时")
        if changed:
            save_state(STATE)


def _reconcile_sentinel_event(message):
    """固件离座哨兵事件 vs 桥接视觉对账（2026-10-09，10-10 升级派单拍照）。

    固件哨兵只看动作差（15 秒一帧），后仰/低头易把"人还在"误判成"离座"，
    离座期间判定区里远处同事的动静还会编出"回归"幻影（2026-10-09 实测）。
    哨兵事件先送这里复核：
      - 与视觉近期人脸证据矛盾 → 拦截（veto）；
      - 无近期人脸证据可判 → 暂扣（hold）并派单让设备立即拍照，
        由 _check_pending_return 用复核照定案（见该函数注释）。
      - "回到座位"：视觉近期一直见人脸 → 用户根本没走过，拦截播报；
      - "离开了座位"：视觉近期见人脸 → 疑似误判，拦截提醒；
      - 视觉证据过时或与事件一致 → 放行，并对账久坐计时
        （放行离座 → 计时暂停；放行回归 → 计时重新起算）。
    副作用：短离座（<约 6 分钟）天然被静默成"计数不打扰"，与喝水策略一致。
    返回 "veto"（拦截）或 "accept"（放行）。
    """
    vision_sees_user = _vision_recently_sees_user()
    if "回到座位" in message:
        if vision_sees_user:
            log("哨兵对账：报'回到座位'但视觉近期持续见人脸 → 判误报，已拦截播报")
            return "veto"
        # 幻影回归防御（2026-10-09 晚）：用户实测离座期间哨兵仍连报
        # "回归→离座→回归"（判定区里远处同事的动静骗过了纯动作差检测）。
        # 眼里没有近期人脸证据时不再直接放行播报，而是暂扣，等下一张
        # 判定照片验明正身（见 _check_pending_return）。
        now_iso = datetime.now().isoformat(timespec="seconds")
        with _state_lock:
            old = STATE.get("pending_return")
            if old:
                log("哨兵对账：又一条'回归'，覆盖暂扣中的上一条")
            STATE["pending_return"] = {"ts": now_iso, "message": message}
            save_state(STATE)
        log("哨兵对账：报'回归'但无近期人脸证据 → 暂扣播报，立即拍照复核")
        _request_verify_photo()
        return "hold"
    if "离开了座位" in message:
        if vision_sees_user:
            log("哨兵对账：报'离座'但视觉近期见人脸 → 判误报，已拦截提醒")
            return "veto"
        now_iso = datetime.now().isoformat(timespec="seconds")
        with _state_lock:
            since = _parse_ts(STATE.get("in_seat_since") or "")
            # 在座秒数先不入账：等复核照确认是真离座再结算（见
            # _check_pending_return），误报离座时计时无缝恢复
            seg = int((datetime.now() - since).total_seconds()) if since else 0
            STATE["in_seat_since"] = None
            # 新的离座事件说明暂扣中的"回归"是幻影（人没回来过就谈不上再离开）
            if STATE.get("pending_return"):
                STATE["pending_return"] = None
                log("哨兵对账：离座事件到达，暂扣中的'回归'判幻影丢弃")
            # 2026-10-27：重复的离座事件不得覆盖已有离座起点——否则「16:14
            # 已走、16:38 哨兵又报一次离座」会把时长算成 4 分钟
            if not STATE.get("away_since"):
                STATE["away_since"] = now_iso
            STATE["pending_away"] = {"ts": now_iso, "seg": seg}
            save_state(STATE)
        log("哨兵对账：报'离座'且无近期人脸证据 → 暂缓结算，立即拍照复核")
        _request_verify_photo()
        return "hold"
    return "accept"


@app.route("/notify", methods=["POST"])
def notify():
    """反向通道入口（P2-⑨）：外部（Hermes 技能、日历轮询、其他程序）推一句
    话进来，机器人会在空闲时用自己的声音念出来。

    body: {"type": "speak"|"alert", "message": "..."}
      speak -> 机器人开口播报 + 屏幕显示
      alert -> 只屏幕显示
    """
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or request.form.to_dict() or {}
    message = (data.get("message") or data.get("text") or "").strip()
    kind = (data.get("type") or "speak").strip().lower()
    source = (data.get("source") or "external").strip()[:32]

    if not message:
        return jsonify({"ok": False, "error": "empty message"}), 400
    if kind not in ("speak", "alert"):
        kind = "speak"
    if len(message) > 2000:
        message = message[:2000]

    # 短时去重：同一内容 90 秒内只收一次（防止重试/重复触发连播）
    import hashlib
    key = hashlib.sha256(f"{kind}:{message}".encode("utf-8")).hexdigest()
    now_ts = time.time()
    for k in [k for k, ts in _notify_seen.items() if now_ts - ts > _NOTIFY_DEDUPE_SECONDS]:
        _notify_seen.pop(k, None)
    if key in _notify_seen:
        return jsonify({"ok": True, "message": "重复消息已忽略（90 秒内去重）",
                        "deduped": True})

    # 哨兵事件对账（2026-10-09）：固件离座哨兵的"离开/回来"先送视觉复核，
    # 与最近人脸判定矛盾时拦截，避免"刚提醒起身又说你回来了"这类打架
    if source == "sentinel":
        verdict = _reconcile_sentinel_event(message)
        if verdict == "veto":
            return jsonify({"ok": True, "vetoed": True,
                            "message": "视觉判定与哨兵事件矛盾，已拦截播报"})
        if verdict == "hold":
            return jsonify({"ok": True, "held": True,
                            "message": "哨兵事件暂扣，已派单拍照复核"})

    with _state_lock:
        reset_daily_if_needed(STATE)
        if kind == "alert":
            STATE["pending"].append({
                "type": "alert",
                "message": message,
                "source": source,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            })
            queued = True
        else:
            queued = push_speak(STATE, message, source=source)
        save_state(STATE)

    if queued:
        _notify_seen[key] = now_ts
    return jsonify({"ok": True, "type": kind, "queued": queued,
                    "message": "已加入播报队列，机器人空闲时会念出来" if queued
                    else "队列已满或今日播报已达上限，未入队"})


@app.route("/pending", methods=["GET"])
def pending():
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    _DEVICE_LAST_SEEN["ts"] = time.time()   # Dashboard：设备在线心跳
    _sweep_verify_timeouts()                # 复核事件惰性超时结算
    with _state_lock:
        items = list(STATE.get("pending", []))
    return jsonify({"ok": True, "items": items})


@app.route("/ack", methods=["POST"])
def ack():
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    idx = data.get("index")
    with _state_lock:
        pend = STATE.get("pending", [])
        if isinstance(idx, int) and 0 <= idx < len(pend):
            pend.pop(idx)
        else:
            STATE["pending"] = []
        save_state(STATE)
    return jsonify({"ok": True})


@app.route("/status", methods=["GET"])
def status():
    with _state_lock:
        st = dict(STATE)
    since = _parse_ts(st.get("in_seat_since"))
    minutes = None
    if since:
        minutes = round((datetime.now() - since).total_seconds() / 60, 1)
    secs = int(st.get("in_seat_seconds_today", 0) or 0)
    if since:
        secs += max(0, int((datetime.now() - since).total_seconds()))
    return jsonify({
        "ok": True,
        "now": datetime.now().isoformat(timespec="seconds"),
        "in_active_window": within_active_window(),
        "presence": st.get("last_presence"),
        "minutes_in_seat": minutes,
        "first_in_seat": st.get("first_in_seat_ts"),
        "last_in_seat": st.get("last_in_seat_ts"),
        "in_seat_minutes_today": round(secs / 60, 1) if secs else 0,
        "samples_today": st.get("samples_today"),
        "reminders_today": st.get("reminders_today"),
        "speaks_today": st.get("speaks_today"),
        "pending_count": len(st.get("pending", [])),
        "calendar_enabled": cfg("calendar", "enabled", default=False),
        "last_vision": st.get("last_vision_ts"),
        "last_photo": st.get("last_photo_meta"),
    })


_auto_resume_timer = None


# ==========================================================================
# Dashboard（2026-09-16，用户提出）：看素材照片 / 日志 / 分析 / 链路状态
# 设计：由 bridge 本体直接服务（单进程单端口，不引新依赖）；
#       Notion 风格极简 UI；只读（v1 不做控制）；仅局域网可访问。
# ==========================================================================

# Dashboard 前端页面已外置为同目录 dashboard.html，便于独立维护
# 每次请求现读（文件仅几十 KB）：改完前端刷新即生效，无需重启桥接
def _dashboard_html() -> str:
    return (Path(__file__).resolve().parent / "dashboard.html").read_text(encoding="utf-8")


@app.route("/dashboard", methods=["GET"])
def dashboard():
    return _dashboard_html()


@app.route("/api/dashboard/summary")
def dashboard_summary():
    with _state_lock:
        st = dict(STATE)
    since = _parse_ts(st.get("in_seat_since"))
    minutes = round((datetime.now() - since).total_seconds() / 60, 1) if since else None
    secs = int(st.get("in_seat_seconds_today", 0) or 0)
    if since:
        secs += max(0, int((datetime.now() - since).total_seconds()))

    seen_ago = time.time() - _DEVICE_LAST_SEEN.get("ts", 0)
    _api_base, _api_model, _api_key = _effective_api_cfg()
    _api_st = _api_settings_load()

    photos = _photos_index(200)
    today = _today()
    photos_today = sum(1 for p in photos if (p.get("ts") or "").startswith(today))

    return jsonify({
        "ok": True,
        "now": datetime.now().isoformat(timespec="seconds"),
        "bridge": {"uptime_s": int(time.time() - _STARTED_AT)},
        "device": {
            "online": seen_ago < 15,
            "last_seen_ago_s": round(seen_ago, 1) if _DEVICE_LAST_SEEN["ts"] else None,
            "last_seen": (datetime.fromtimestamp(_DEVICE_LAST_SEEN["ts"])
                          .isoformat(timespec="seconds")
                          if _DEVICE_LAST_SEEN["ts"] else None),
        },
        "presence": {
            "presence": st.get("last_presence"),
            "minutes_in_seat": minutes,
            "in_seat_minutes_today": round(secs / 60, 1) if secs else 0,
            "first_in_seat": st.get("first_in_seat_ts"),
            "last_in_seat": st.get("last_in_seat_ts"),
            "enabled": cfg("presence", "enabled", default=True),
        },
        "state": {
            "samples_today": st.get("samples_today"),
            "reminders_today": st.get("reminders_today"),
            "speaks_today": st.get("speaks_today"),
            "water_level": st.get("water_level"),
            "water_reminds_today": st.get("water_reminds_today", 0),
            "pending_count": len(st.get("pending", [])),
            "last_vision": st.get("last_vision_ts"),
        },
        "water": {
            "enabled": _effective_water_enabled(),
            "judge_mode": cfg("water", "judge_mode", default="off"),
        },
        "behavior": _behavior_summary(),
        "vision": {
            "mode": _effective_vision_mode(),
            "api_ready": bool(_api_base and _api_model and _api_key),
            "api_provider": _api_st.get("provider", ""),
            "api_model": _api_model,
        },
        "photo_positions": _photo_positions(),
        "power_save": _power_save_cfg(),
        "stability": {
            "last_reset_reason": STATE.get("last_reset_reason"),
            "mem_last": STATE.get("mem_last"),
        },
        "photos": {
            "total": len(photos),
            "today": photos_today,
            "retention_days": cfg("notify", "photo_retention_days", default=3),
        },
        "feishu": {
            "enabled": cfg("notify", "feishu", default=False),
            "last_ts": _FEISHU_LAST.get("ts"),
            "last_ok": _FEISHU_LAST.get("ok"),
            "last_error": _FEISHU_LAST.get("error"),
        },
        "hermes": {"task_timeout": cfg("hermes", "task_timeout", default=600)},
        "calendar": {"enabled": cfg("calendar", "enabled", default=False)},
    })


def _photos_index(limit=200):
    """照片列表：从 analysis.jsonl 取带 file 的行（新→旧），seat 与 cup 混排。"""
    rows = _read_analysis_tail(limit=500)
    out = []
    for r in reversed(rows):          # 新的在前
        if r.get("type") == "vision" and r.get("file"):
            out.append({"ts": r.get("ts"), "file": r["file"],
                        "presence": r.get("presence"), "kind": "seat",
                        "url": f"/photos/{r['file']}"})
        elif r.get("type") == "cup_photo" and r.get("file"):
            out.append({"ts": r.get("ts"), "file": r["file"],
                        "presence": "cup", "kind": "cup",
                        "url": f"/photos/{r['file']}"})
        elif r.get("type") == "extra_photo" and r.get("file"):
            # 扩展拍摄位（2026-09-17 多拍摄位）：kind = 用户起的名字
            out.append({"ts": r.get("ts"), "file": r["file"],
                        "presence": "extra",
                        "kind": r.get("name") or r.get("source") or "扩展位",
                        "url": f"/photos/{r['file']}"})
        if len(out) >= limit:
            break
    return out


@app.route("/api/dashboard/photos")
def dashboard_photos():
    limit = min(int(request.args.get("limit", 24)), 100)
    photos = _photos_index(limit=200)

    # 把同一时刻的喝水判定挂到照片上（ts 对齐）
    water_by_ts = {r.get("ts"): r.get("level")
                   for r in _read_analysis_tail(limit=500)
                   if r.get("type") == "water"}
    for p in photos:
        p["level"] = water_by_ts.get(p["ts"])
    return jsonify({"ok": True, "total": len(photos),
                    "photos": photos[:limit]})


@app.route("/api/dashboard/analysis")
def dashboard_analysis():
    limit = min(int(request.args.get("limit", 30)), 200)
    rows = _read_analysis_tail(limit=limit)
    rows.reverse()                     # 新的在前
    return jsonify({"ok": True, "rows": rows})


@app.route("/api/dashboard/logs")
def dashboard_logs():
    lines_n = min(int(request.args.get("lines", 120)), 500)
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        tail = [l.rstrip("\n") for l in all_lines[-lines_n:]]
    except OSError:
        tail = ["（日志文件不存在）"]
    return jsonify({"ok": True, "lines": tail})


@app.route("/photos/<path:name>")
def photos_serve(name):
    safe = Path(name).name            # 只允许纯文件名，防目录穿越
    if not safe.endswith(".jpg"):
        return "not found", 404
    path = PHOTO_DIR / safe
    if not path.exists():
        return "not found", 404
    return send_file(path, mimetype="image/jpeg")


@app.route("/api/dashboard/api_settings", methods=["GET"])
def dashboard_api_settings_get():
    st = _api_settings_load()
    return jsonify({
        "ok": True,
        "provider": st.get("provider", ""),
        "base_url": st.get("base_url", ""),
        "model": st.get("model", ""),
        "api_key_set": bool(st.get("api_key")),
        "water_enabled": _effective_water_enabled(),
    })


@app.route("/api/dashboard/api_settings", methods=["POST"])
def dashboard_api_settings_post():
    data = request.get_json(silent=True) or {}
    st = _api_settings_load()
    # provider/base_url/model 覆盖写；api_key 留空 = 保留旧 key（方便只换厂商）
    for k in ("provider", "base_url", "model"):
        if k in data:
            st[k] = str(data[k]).strip()
    key = str(data.get("api_key", "")).strip()
    if key:
        st["api_key"] = key
    if "water_enabled" in data:
        st["water_enabled"] = bool(data["water_enabled"])
    st["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _api_settings_save(st)
    log(f"视觉 API 设置已更新（provider={st.get('provider', '?')}，"
        f"model={st.get('model', '?')}，key={'已设置' if st.get('api_key') else '未设置'}，"
        f"喝水判定={'开' if st.get('water_enabled') else '关'}）")
    return jsonify({"ok": True,
                    "api_key_set": bool(st.get("api_key"))})


@app.route("/api/dashboard/api_test", methods=["POST"])
def dashboard_api_test():
    """用一张小测试图真调一次 API，验证 key/地址/模型连通性。"""
    import cv2
    import numpy as np
    img = np.full((64, 64, 3), 200, dtype=np.uint8)
    cv2.circle(img, (32, 32), 16, (60, 120, 230), -1)
    ok, enc = cv2.imencode(".jpg", img)
    if not ok:
        return jsonify({"ok": False, "error": "测试图编码失败"}), 500
    t0 = time.time()
    call_ok, text, err = _vision_api_call(enc.tobytes(), "这是一张测试图，请只回答 OK")
    return jsonify({"ok": call_ok, "reply": text[:60],
                    "latency_s": round(time.time() - t0, 1),
                    "error": err})


@app.route("/api/dashboard/photo_positions", methods=["GET"])
def photo_positions_get():
    return jsonify({"ok": True, "photo_positions": _photo_positions()})


@app.route("/api/dashboard/photo_positions", methods=["POST"])
def photo_positions_post():
    """保存拍摄位（免烧录）。设备在下一个拍照周期经 /device_config 拉取并写入 NVS。"""
    data = request.get_json(silent=True) or {}
    seat = data.get("seat") or {}
    cup = data.get("cup") or {}
    try:
        new_pos = {
            "seat": {"yaw": int(seat["yaw"]), "pitch": int(seat["pitch"])},
            "cup": {"yaw": int(cup["yaw"]), "pitch": int(cup["pitch"])},
        }
    except (KeyError, TypeError, ValueError):
        return jsonify({"ok": False, "error": "参数缺失或不是整数（seat/cup 各含 yaw/pitch）"}), 400
    # 与固件 set_head_angles 工具同界：Yaw -128~+128，Pitch 0~90
    for name, p in new_pos.items():
        if not (-128 <= p["yaw"] <= 128 and 0 <= p["pitch"] <= 90):
            return jsonify({"ok": False,
                            "error": f"{name} 角度越界：yaw 需 -128~128，pitch 需 0~90"}), 400
    # 扩展拍摄位（2026-09-17 多拍摄位）：最多 3 个，{name,yaw,pitch}
    raw_extras = data.get("extras") or []
    if not isinstance(raw_extras, list) or len(raw_extras) > 3:
        return jsonify({"ok": False, "error": "extras 最多 3 个拍摄位"}), 400
    extras = []
    for i, e in enumerate(raw_extras):
        try:
            nm = str(e.get("name", "")).strip()[:10]
            y, p2 = int(e["yaw"]), int(e["pitch"])
        except (KeyError, TypeError, ValueError):
            return jsonify({"ok": False,
                            "error": f"扩展拍摄位 #{i+1} 缺 name/yaw/pitch 或不是整数"}), 400
        if not nm:
            return jsonify({"ok": False, "error": f"扩展拍摄位 #{i+1} 名字不能为空"}), 400
        if not (-128 <= y <= 128 and 0 <= p2 <= 90):
            return jsonify({"ok": False,
                            "error": f"「{nm}」角度越界：yaw 需 -128~128，pitch 需 0~90"}), 400
        extras.append({"name": nm, "yaw": y, "pitch": p2})
    new_pos["extras"] = extras
    st = _device_settings_load()
    st["photo_positions"] = new_pos
    st["ver"] = int(st.get("ver", 0)) + 1
    _device_settings_save(st)
    log(f"拍摄位已更新（ver={st['ver']}）：seat yaw={new_pos['seat']['yaw']} "
        f"pitch={new_pos['seat']['pitch']} / cup yaw={new_pos['cup']['yaw']} "
        f"pitch={new_pos['cup']['pitch']} / 扩展位 {len(extras)} 个，设备下个拍照周期生效")
    return jsonify({"ok": True, "photo_positions": _photo_positions()})


@app.route("/api/dashboard/power_save", methods=["GET"])
def power_save_get():
    return jsonify({"ok": True, "power_save": _power_save_cfg()})


@app.route("/api/dashboard/power_save", methods=["POST"])
def power_save_post():
    """保存空闲休眠配置（免烧录）。设备在下一个拍照周期经 /device_config 拉取生效。"""
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    try:
        minutes = int(data.get("idle_sleep_minutes"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "idle_sleep_minutes 需为整数分钟"}), 400
    if not (1 <= minutes <= 720):
        return jsonify({"ok": False, "error": "空闲时长需在 1~720 分钟之间"}), 400
    st = _device_settings_load()
    st["power_save"] = {"enabled": enabled, "idle_sleep_minutes": minutes}
    _device_settings_save(st)
    state_txt = "开启" if enabled else "关闭（陪伴模式常醒）"
    log(f"空闲休眠配置已更新：{state_txt}，空闲 {minutes} 分钟休眠，设备下个拍照周期生效")
    return jsonify({"ok": True, "power_save": _power_save_cfg()})


# 设备复位原因码 → 名称（esp_reset_reason()，IDF v6.0.3 esp_system.h 枚举从 0 起）
# 2026-09-16 实录：首版按老 ESP32 编号写错（PANIC 写成 3），烧录硬复位报 11=USB
# 被误报成异常——码表必须对照当前 IDF 头文件，不能凭记忆。
_RST_NAMES = {
    0: "UNKNOWN(未知)",
    1: "POWERON(上电)",
    2: "EXT(外部引脚复位)",
    3: "SW(软件重启)",
    4: "PANIC(崩溃)",
    5: "INT_WDT(中断看门狗)",
    6: "TASK_WDT(任务看门狗)",
    7: "WDT(其他看门狗)",
    8: "DEEPSLEEP(深睡唤醒)",
    9: "BROWNOUT(电压骤降)",
    10: "SDIO",
    11: "USB(烧录/调试复位)",
    12: "JTAG",
    13: "EFUSE",
    14: "PWR_GLITCH(电源毛刺)",
    15: "CPU_LOCKUP(双异常锁死)",
}
# 异常复位：崩溃 / 看门狗 / 掉电类
_RST_BAD = {0, 4, 5, 6, 7, 9, 14, 15}


@app.route("/device_config", methods=["GET"])
def device_config():
    """设备轮询的配置下发口。带 ver；设备 ver 相同就不用解析/写 NVS。

    2026-09-16 加：设备随请求上报稳定性遥测（rst/fi/fb/fp/up）。
    新开机（up<300s）的第一拉取 = 上一次运行的"死因"自白：
      PANIC/看门狗/BROWNOUT = 异常重启（正是"自动退出 Agent 模式"的远程证据）
      POWERON/SW_RESET = 正常。内存水位随轮次记录，长期看是否走低（内存墙假设）。
    """
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    pos = _photo_positions()

    # ---- 稳定性遥测 ----
    now = datetime.now()
    rst = request.args.get("rst", type=int)
    if rst is not None:
        up = request.args.get("up", type=int, default=9999)
        fi = request.args.get("fi", type=int)
        fb = request.args.get("fb", type=int)
        fp = request.args.get("fp", type=int)
        name = _RST_NAMES.get(rst, f"未知({rst})")
        with _state_lock:
            last_boot = STATE.get("_last_boot_log_ts")
            is_fresh_boot = up is not None and up < 300
            # 每次新开机只记一次（配置拉取每 5 分钟一轮，up 只在开机后 <300s 内为小值）
            if is_fresh_boot and (last_boot is None
                                  or (now - datetime.fromisoformat(last_boot)).total_seconds() > 120):
                STATE["_last_boot_log_ts"] = now.isoformat(timespec="seconds")
                STATE["last_reset_reason"] = {"code": rst, "name": name,
                                              "ts": now.isoformat(timespec="seconds")}
                if rst in _RST_BAD:
                    log(f"⚠️ 设备异常重启上报：复位原因={name} —— 这就是一次"
                        f"{'崩溃' if rst in (4, 15) else '看门狗/掉电' if rst in (5, 6, 7, 9, 14) else '未知异常'}退出",
                        "WARN")
                else:
                    log(f"设备新开机上报：复位原因={name}（正常）")
        if fi is not None and fb is not None:
            with _state_lock:
                series = [x for x in (STATE.get("mem_series") or [])
                          if isinstance(x, list) and len(x) == 3]
                series.append([now.isoformat(timespec="seconds"), fi, fb])
                STATE["mem_series"] = series[-72:]   # 最近 6 小时（5 分钟一轮）
                STATE["mem_last"] = {"internal_free": fi, "internal_largest": fb,
                                     "psram_free": fp,
                                     "ts": now.isoformat(timespec="seconds")}

    log(f"device_config: 设备拉取配置（ver={pos['ver']}）")
    return jsonify({"ok": True, "ver": pos["ver"], "photo_positions": pos,
                    "power_save": _power_save_cfg()})


# ==========================================================================

@app.route("/pause", methods=["POST"])
def pause():
    """一键暂停/恢复久坐提醒与日程播报（隐私、开会或不想被打扰时用）

    body: {"enabled": false, "minutes": 60}
    minutes 可选：暂停 N 分钟后自动恢复（会议模式 P2-⑪）。
    """
    global _auto_resume_timer
    if not check_token():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled", True))
    minutes = data.get("minutes")
    with _config_lock:
        CFG["presence"]["enabled"] = enabled
    log(f"提醒已{'开启' if enabled else '暂停'}")

    auto_resume_at = None
    if not enabled:
        # 有定时则安排自动恢复；重复调用时以最后一次为准
        if _auto_resume_timer is not None:
            _auto_resume_timer.cancel()
            _auto_resume_timer = None
        try:
            minutes = int(minutes) if minutes is not None else None
        except (TypeError, ValueError):
            minutes = None
        if minutes and minutes > 0:
            auto_resume_at = (datetime.now()
                              + timedelta(minutes=minutes)
                              ).isoformat(timespec="seconds")

            def _auto_resume():
                with _config_lock:
                    CFG["presence"]["enabled"] = True
                log(f"会议模式结束，提醒已自动恢复（暂停了 {minutes} 分钟）")

            _auto_resume_timer = threading.Timer(minutes * 60, _auto_resume)
            _auto_resume_timer.daemon = True
            _auto_resume_timer.start()
            log(f"将在 {auto_resume_at} 自动恢复提醒")
    else:
        if _auto_resume_timer is not None:
            _auto_resume_timer.cancel()
            _auto_resume_timer = None
    return jsonify({"ok": True, "enabled": enabled,
                    "auto_resume_at": auto_resume_at})


# --------------------------------------------------------------------------

def main():
    host = cfg("server", "host", default="0.0.0.0")
    port = int(cfg("server", "port", default=8787))
    log("=" * 56)
    log("StackChan 桥接服务启动")
    log(f"  监听地址   : http://{host}:{port}")
    log(f"  视觉判定   : {_effective_vision_mode()}（Dashboard 配齐 API 自动走云端）")
    log(f"  久坐提醒   : {'开' if cfg('presence', 'enabled') else '关'}"
        f"（连续 {cfg('presence', 'remind_after_minutes')} 分钟）")
    log(f"  飞书推送   : {'开' if cfg('notify', 'feishu') else '关'}")
    log(f"  照片落盘   : {'开' if cfg('notify', 'save_photos') else '关'}")
    log(f"  数据目录   : {DATA_DIR}")
    log("=" * 56)

    if cfg("presence", "enabled", default=True):
        threading.Thread(target=monitor_loop, daemon=True).start()
        log("已启动久坐提醒监控线程")

    if cfg("calendar", "enabled", default=False):
        threading.Thread(target=calendar_loop, daemon=True).start()
        log(f"已启动日历播报线程（提前 {cfg('calendar', 'lead_minutes')} 分钟）")
    else:
        log("日历播报未启用（calendar.enabled=false）。"
            "启用方法：config.yaml 里 calendar.enabled=true 并配置 fetch_cmd")

    if cfg("daily_summary", "enabled", default=True):
        threading.Thread(target=daily_summary_loop, daemon=True).start()
        log(f"已启动下班汇总线程（每天 "
            f"{cfg('daily_summary', 'hour')}:{cfg('daily_summary', 'minute'):02d}）")

    if not cfg("hermes", "feishu_target", default=""):
        log("提示：hermes.feishu_target 为空，飞书推送会被跳过。"
            "用 `hermes send --list` 查看可用目标。", "WARN")

    # 跟踪开关状态恢复（2026-10-17）：此前 enabled 是内存态，桥接/电脑重启
    # 就丢（今早电脑开机后"在座判定看到人也不重启"即此因）。从 state.json
    # 恢复并立即补发开启指令（设备已在跟则 no-op）。
    # 2026-10-21：先跑舵机安全自检，限位配置不合法（包不住归位姿态）时拒绝
    # 自动恢复，防止一开机就把舵机往限位顶。
    _safety_ok = _safety_startup_check()
    if STATE.get("tracking_enabled") and not _safety_ok:
        with _state_lock:
            STATE["tracking_enabled"] = False
            save_state(STATE)
        log("跟踪自动恢复已拦截：请先修正 tracking 限位配置", "ERROR")
    if STATE.get("tracking_enabled"):
        tc = _track_cfg()
        TRACK["enabled"] = True
        TRACK["active"] = True
        TRACK["angle"] = tc["home_pan"]
        TRACK["tilt"] = tc["home_tilt"]
        TRACK["last_frame_ts"] = time.time()
        with _state_lock:
            has = any(p.get("type") == "tracking" and p.get("message") == "on"
                      for p in STATE.get("pending", []))
        if not has:
            _push_pending_command("tracking", "on")
        log("人脸跟踪：检测到上次为开启状态，已恢复（补发开启指令）")

    threading.Thread(target=_tracking_keepalive, daemon=True,
                     name="track-keepalive").start()
    threading.Thread(target=_track_stream_server, daemon=True,
                     name="track-stream-accept").start()
    app.run(host=host, port=port, threaded=True, debug=False)


if __name__ == "__main__":
    # 崩溃留痕：pythonw 下 stderr 为 None，任何未捕获异常/原生崩溃都无声消失。
    # faulthandler 把段错误等原生崩溃的调用栈写进 bridge_crash.log，便于追溯。
    try:
        import faulthandler
        _crash = open(os.path.join(DATA_DIR, "bridge_crash.log"),
                      "a", encoding="utf-8", errors="replace")
        _crash.write("\n[%s] ---- bridge start (pid=%d) ----\n"
                     % (time.strftime("%Y-%m-%d %H:%M:%S"), os.getpid()))
        _crash.flush()
        if sys.stderr is None:
            sys.stderr = _crash
        if sys.stdout is None:
            sys.stdout = _crash
        faulthandler.enable(file=_crash)
    except Exception:
        pass
    main()
