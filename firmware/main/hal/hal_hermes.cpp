/*
 * SPDX-FileCopyrightText: 2026 Robin / StackChan 改造项目
 * SPDX-License-Identifier: MIT
 *
 * hal_hermes —— StackChan ↔ 本机 Hermes Agent 桥接
 *
 * ⚠️ 说明：这是参考实现，接口与调用方式均按官方 v1.5.1 源码的实际写法编写，
 *    但需要在装好 ESP-IDF 5.5.4 的环境里编译验证。首次编译若报错，
 *    多数是头文件或 API 名称随小智版本变动，按注释里的提示微调即可。
 */

#include "hal_hermes.h"

#include <hal/hal.h>
#include <hal/board/hal_bridge.h>
#include <application.h>
#include <board.h>
#include <mooncake_log.h>
#include <mcp_server.h>
#include <stackchan/stackchan.h>
#include <apps/common/common.h>
#include <assets/assets.h>
#include <jpg/image_to_jpeg.h>

#include <cJSON.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <esp_heap_caps.h>
#include <esp_netif.h>
#include <esp_system.h>
#include <esp_timer.h>
#include <nvs.h>

#include <string>
#include <vector>
#include <cstring>

/* ========================================================================== */
/*                                  配置                                       */
/*                                                                            */
/* 在 main/Kconfig.projbuild 里加一个 menu "StackChan Bridge"：                 */
/*                                                                            */
/*   config STACKCHAN_BRIDGE_URL                                               */
/*       string "Bridge server base URL"                                       */
/*       default "http://<bridge-lan-ip>:8787"                                  */
/*   config STACKCHAN_BRIDGE_TOKEN                                             */
/*       string "Bridge device token (optional)"                               */
/*       default ""                                                            */
/*   config STACKCHAN_POLL_INTERVAL_SEC                                        */
/*       int "Poll /pending interval (seconds)"                                */
/*       default 30                                                            */
/*   config STACKCHAN_PHOTO_INTERVAL_MIN                                       */
/*       int "Auto photo upload interval (minutes, 0=off)"                     */
/*       default 5                                                             */
/*                                                                            */
/* 先在代码里用 #ifndef 兜底，这样不加 Kconfig 也能编译。                       */
/* ========================================================================== */

#ifndef CONFIG_STACKCHAN_BRIDGE_URL
// 兜底值：正常应该由 sdkconfig.defaults.local 提供。
// ⚠️ 这是"因机器而变"的地址，改前先跑 ipconfig getifaddr en0 确认。
#define CONFIG_STACKCHAN_BRIDGE_URL "http://192.168.1.100:8787"
#endif
#ifndef CONFIG_STACKCHAN_BRIDGE_TOKEN
#define CONFIG_STACKCHAN_BRIDGE_TOKEN ""
#endif
#ifndef CONFIG_STACKCHAN_POLL_INTERVAL_SEC
#define CONFIG_STACKCHAN_POLL_INTERVAL_SEC 30
#endif
#ifndef CONFIG_STACKCHAN_PHOTO_INTERVAL_MIN
#define CONFIG_STACKCHAN_PHOTO_INTERVAL_MIN 5
#endif

namespace hal_hermes {

static constexpr std::string_view _tag = "HERMES";

/* ========================================================================== */
/*                              HTTP 小工具                                    */
/* ========================================================================== */

static std::string _base_url()
{
    std::string url = CONFIG_STACKCHAN_BRIDGE_URL;
    // 去掉结尾斜杠，拼接时统一用 /path
    while (!url.empty() && url.back() == '/') {
        url.pop_back();
    }
    return url;
}

/** 简单 POST。body 走 chunked（与官方 stackchan_camera.cc 的写法一致）。 */
static bool http_post(const std::string& path, const std::string& body, const char* content_type,
                      std::string& response)
{
    auto& board  = Board::GetInstance();
    auto network = board.GetNetwork();
    if (!network) {
        mclog::tagError(_tag, "network not ready");
        return false;
    }

    auto http = network->CreateHttp(3);
    if (!http) {
        mclog::tagError(_tag, "failed to create http client");
        return false;
    }

    const std::string url = _base_url() + path;
    http->SetHeader("Content-Type", content_type);

    const std::string token = CONFIG_STACKCHAN_BRIDGE_TOKEN;
    if (!token.empty()) {
        http->SetHeader("X-Device-Token", token);
    }
    http->SetHeader("Device-Id", GetHAL().getFactoryMacString(":"));

    if (!http->Open("POST", url)) {
        mclog::tagError(_tag, "open failed: {}", url);
        return false;
    }

    // body 分块写入，最后用空块结束（chunked）
    http->Write(body.c_str(), body.size());
    http->Write("", 0);

    const int status = http->GetStatusCode().value_or(-1);
    response         = http->ReadAll();
    http->Close();

    if (status != 200) {
        mclog::tagWarn(_tag, "POST {} -> {}", url, status);
        return false;
    }
    return true;
}

static bool http_post_json(const std::string& path, const std::string& json, std::string& response)
{
    return http_post(path, json, "application/json", response);
}

static bool http_get(const std::string& path, std::string& response)
{
    auto& board  = Board::GetInstance();
    auto network = board.GetNetwork();
    if (!network) {
        return false;
    }

    auto http = network->CreateHttp(3);
    if (!http) {
        return false;
    }

    const std::string url   = _base_url() + path;
    const std::string token = CONFIG_STACKCHAN_BRIDGE_TOKEN;
    if (!token.empty()) {
        http->SetHeader("X-Device-Token", token);
    }

    if (!http->Open("GET", url)) {
        return false;
    }
    const int status = http->GetStatusCode().value_or(-1);
    response         = http->ReadAll();
    http->Close();
    return status == 200;
}

/* ========================================================================== */
/*                     本地语音播报（2026-09-14 新路线）                        */
/*                                                                            */
/*  ⚠️ 为什么不用 WakeWordInvoke 注入文字：                                    */
/*  2026-09-14 真机实测（长/短文本各一轮，串口日志一致）：                      */
/*  SendWakeWordDetected 把文字作为 {"type":"listen","state":"detect"} 发给    */
/*  小智云端后，云端只把它当"误唤醒"处理——设备进入 listening 等真人开口，      */
/*  65 秒无语音后云端说声告别收场。注入的文字从头到尾没有进 LLM。              */
/*  因此"云端 TTS 播报"路线不成立，改为：                                     */
/*  桥接用 edge-tts 合成 OGG opus(24kHz mono) → 机器人下载到内存 →             */
/*  AudioService::PlaySound 本地喇叭直接播（与官方提示音同一管线）。           */
/* ========================================================================== */

static void show_alert(const std::string& message);   // 前置声明，定义在下方

static std::string _url_encode(const std::string& s)
{
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size() * 3);
    for (const unsigned char c : s) {
        const bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                                (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                                c == '.' || c == '~';
        if (unreserved) {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 0x0F];
        }
    }
    return out;
}

/**
 * @brief 让机器人"开口"念一段文字（notify 流式播报，v1.9.8）
 *
 * 向桥接请求 /tts/url?text=... 拿到一个音频 URL（首次约 2~5 秒现场合成，
 * 之后命中缓存），然后交给官方 notify 引擎（移植自 PR #2191）流式播放：
 * 设备只持有 URL，HTTP 增量解封装 + 共享有界解码队列（20 包 ≈ 1.2 秒），
 * 内存占用与音频时长解耦，不再整文件下载。
 *
 * ⚠️ 只允许在 hermes_tts 专用任务里调用：speak 时设备必须处于 idle
 *    （notify 引擎的要求），PlayNotifyUrl 忙时会直接返回 false。
 *
 * @return true 已提交播放（异步）；false 拿 URL 失败或设备忙，
 *         调用方应回退为本地提示音 + 屏幕显示。
 */
static bool speak_via_notify(const std::string& text)
{
    if (text.empty()) {
        return false;
    }
    // 120 字约 25~30 秒音频，限制合成时长
    std::string clipped = text.substr(0, 120);

    // v1.9.12：/tts/url 冷缓存时桥接要现场合成 10~15s，可能撞上设备 HTTP
    // 超时（实测"叮咚了但没语音"就是这条：fetch failed → 兜底叮咚）。
    // 桥接已在入队时预热缓存，这里再兜 3 次重试，间隔 2s。
    std::string resp;
    bool fetched = false;
    for (int attempt = 1; attempt <= 3 && !fetched; attempt++) {
        if (attempt > 1) {
            vTaskDelay(pdMS_TO_TICKS(2000));
        }
        fetched = http_get("/tts/url?text=" + _url_encode(clipped), resp);
        if (!fetched) {
            mclog::tagWarn(_tag, "tts url fetch failed (attempt {}/3)", attempt);
        }
    }
    if (!fetched) {
        return false;
    }

    bool ok = false;
    cJSON* root = cJSON_Parse(resp.c_str());
    if (root != nullptr) {
        const cJSON* url_item = cJSON_GetObjectItem(root, "url");
        if (cJSON_IsString(url_item) && url_item->valuestring[0] != '\0') {
            std::string url = url_item->valuestring;
            mclog::tagInfo(_tag, "notify speak: {} bytes url, streaming", url.size());
            ok = Application::GetInstance().PlayNotifyUrl(std::move(url));
            if (!ok) {
                mclog::tagWarn(_tag, "PlayNotifyUrl rejected (device busy?)");
            }
        } else {
            mclog::tagWarn(_tag, "tts url response missing url field");
        }
        cJSON_Delete(root);
    } else {
        mclog::tagWarn(_tag, "tts url response not JSON: {}B", (int)resp.size());
    }
    return ok;
}

/* ========================================================================== */
/*                        TTS 专用任务（v1.9.5）                                */
/*                                                                            */
/*  为什么要独立任务：PlaySound 对长音频会阻塞在解码队列背压上（等播放实时腾位）， */
/*  下载还可能撞上网络慢。放轮询循环里会拖死 /pending 轮询和 /ack（v1.9.4 实测）。 */
/*  独立任务后，语音再慢也只影响语音自己，轮询/拍照/派单照常跑。                  */
/* ========================================================================== */

struct TtsRequest_t {
    char text[512];   // 截到 120 个汉字，UTF-8 最多 ~360B，512 足够
};

static QueueHandle_t _tts_queue = nullptr;

static void tts_worker(void*)
{
    // 2026-09-14 深夜结论：本地 TTS 播报在这块板上不可行——
    // AudioService 解码队列的包全在内部 RAM 上，30 秒音频 ~450 包 ≈ 110KB，
    // 会把内部 RAM 榨干（实测 largest block 跌到 10KB），连累唤醒词引擎
    // 和小智云连接（症状：灯不亮不理人、反复 connecting 失败、无提示音）。
    //
    // 2026-09-15 v1.9.8 改用官方 notify 引擎（移植自 78/xiaozhi-esp32 PR #2191）：
    // 桥接只下发一个音频 URL，设备走 NotifyPlayer 流式边下边播，共用同一条
    // 有界解码队列（上限 20 包 ≈ 1.2 秒），内存占用与音频时长解耦。
    static constexpr bool kNotifySpeakEnabled = true;

    TtsRequest_t req{};
    while (true) {
        if (xQueueReceive(_tts_queue, &req, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        std::string text(req.text);
        show_alert(text);   // 文字上屏：路过看一眼就知道
        bool played = false;
        if (kNotifySpeakEnabled) {
            played = speak_via_notify(text);
        }
        if (!played) {
            // 兜底提示音：叮咚一声（这条管线短音频实测稳定）
            hal_bridge::app_play_sound(OGG_NEW_NOTIFICATION);
            // v1.9.12：若这次是插播（退会话后播放），叮咚放完要恢复监听。
            // OnBroadcastFallbackFinished 内部有标记判断，普通播报是空操作。
            Application::GetInstance().OnBroadcastFallbackFinished();
        }
    }
}

/** 各处播报统一走这里：丢进 TTS 任务队列就返回，绝不阻塞调用线程 */
static bool request_speak(const std::string& message)
{
    if (message.empty() || _tts_queue == nullptr) {
        return false;
    }
    TtsRequest_t req{};
    const size_t n = std::min(message.size(), sizeof(req.text) - 1);
    std::memcpy(req.text, message.data(), n);
    req.text[n] = '\0';
    return xQueueSend(_tts_queue, &req, 0) == pdTRUE;
}

/* ========================================================================== */
/*                          拍照 + multipart 上传                              */
/* ========================================================================== */

/* JPEG 输出缓冲：固定 PSRAM 缓冲，不再用 std::vector。
 * 2026-09-16 实录：vector ~15KB 的分配走内部 RAM，内存碎片化后最大连续块
 * 只剩 7.5KB，直接把编码路径堵死（拍照轮全部被守卫拦下）。PSRAM 96KB 富余。 */
static uint8_t* s_jpeg_buf  = nullptr;
static constexpr size_t _JPEG_BUF_CAP = 96 * 1024;
static size_t s_jpeg_len    = 0;

static size_t _jpeg_sink(void* arg, size_t index, const void* data, size_t len)
{
    (void)arg;
    if (index == 0 && data != nullptr && len > 0) {
        if (s_jpeg_buf == nullptr || len > _JPEG_BUF_CAP) {
            return 0;   // 编码器输出超限，视为失败（不会写爆缓冲）
        }
        memcpy(s_jpeg_buf, data, len);
        s_jpeg_len = len;
    }
    return len;
}

/** 抓一帧并编码成 JPEG（复用官方硬件优化的编码器），输出在 s_jpeg_buf/s_jpeg_len */
static bool capture_jpeg(size_t& out_len)
{
    if (s_jpeg_buf == nullptr) {
        s_jpeg_buf = static_cast<uint8_t*>(heap_caps_malloc(_JPEG_BUF_CAP, MALLOC_CAP_SPIRAM));
        if (s_jpeg_buf == nullptr) {
            mclog::tagWarn(_tag, "jpeg psram buffer alloc failed");
            return false;
        }
    }
    s_jpeg_len = 0;

    auto* camera = hal_bridge::board_get_camera();
    if (camera == nullptr) {
        mclog::tagWarn(_tag, "camera not available");
        return false;
    }
    if (!camera->Capture()) {
        mclog::tagWarn(_tag, "camera capture failed");
        return false;
    }

    const uint16_t w = camera->GetFrameWidth() ? camera->GetFrameWidth() : 320;
    const uint16_t h = camera->GetFrameHeight() ? camera->GetFrameHeight() : 240;

    const bool ok = image_to_jpeg_cb(const_cast<uint8_t*>(camera->GetFrameData()),
                                     camera->GetFrameSize(), w, h,
                                     static_cast<v4l2_pix_fmt_t>(camera->GetFrameFormat()), 80,
                                     _jpeg_sink, nullptr);
    if (!ok || s_jpeg_len == 0) {
        mclog::tagWarn(_tag, "jpeg encode failed");
        return false;
    }
    out_len = s_jpeg_len;
    return true;
}

/**
 * @brief 把一张照片传到桥接服务的 /vision
 *
 * 请求格式与官方 StackChanCamera::Explain() 完全一致
 * （multipart/form-data，字段 question + file），
 * 所以桥接服务端不需要为两套格式写两份解析。
 */
static bool upload_photo(const std::string& question, std::string& answer,
                         const char* photo_type = "seat")
{
    size_t jpeg_len = 0;
    if (!capture_jpeg(jpeg_len)) {
        return false;
    }

    auto& board  = Board::GetInstance();
    auto network = board.GetNetwork();
    if (!network) {
        return false;
    }
    auto http = network->CreateHttp(3);
    if (!http) {
        return false;
    }

    static const std::string boundary = "----STACKCHAN_HERMES_BOUNDARY";
    const std::string url             = _base_url() + "/vision";

    http->SetHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
    http->SetHeader("Transfer-Encoding", "chunked");
    http->SetHeader("Device-Id", GetHAL().getFactoryMacString(":"));
    // 照片用途标记（2026-09-16 双拍摄位）：seat=在座判定 / cup=水杯水位判定
    http->SetHeader("X-Photo-Type", photo_type);
    const std::string token = CONFIG_STACKCHAN_BRIDGE_TOKEN;
    if (!token.empty()) {
        http->SetHeader("X-Device-Token", token);
    }

    if (!http->Open("POST", url)) {
        mclog::tagError(_tag, "open failed: {}", url);
        return false;
    }

    // 第一部分：question
    const std::string q_field = "--" + boundary +
                                "\r\nContent-Disposition: form-data; name=\"question\"\r\n\r\n" + question +
                                "\r\n";
    http->Write(q_field.c_str(), q_field.size());

    // 第二部分：文件头
    const std::string f_header = "--" + boundary +
                                 "\r\nContent-Disposition: form-data; name=\"file\"; "
                                 "filename=\"camera.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n";
    http->Write(f_header.c_str(), f_header.size());

    // 第三部分：JPEG 数据（固定 PSRAM 缓冲，不占内部 RAM）
    http->Write(reinterpret_cast<const char*>(s_jpeg_buf), jpeg_len);

    // 第四部分：结尾
    const std::string footer = "\r\n--" + boundary + "--\r\n";
    http->Write(footer.c_str(), footer.size());

    http->Write("", 0);

    const int status = http->GetStatusCode().value_or(-1);
    answer           = http->ReadAll();
    http->Close();

    if (status != 200) {
        mclog::tagWarn(_tag, "upload photo -> {}", status);
        return false;
    }

    mclog::tagInfo(_tag, "photo {} bytes uploaded, answer: {}", (int)jpeg_len, answer);
    return true;
}

/* ========================================================================== */
/*                          派发任务的异步队列                                  */
/* ========================================================================== */

struct DispatchRequest_t {
    char command[512];
};

static QueueHandle_t _dispatch_queue = nullptr;

/**
 * @brief 把命令丢进队列，后台任务去发 HTTP
 *
 * 为什么不直接在 MCP 工具里发 HTTP：
 * MCP 工具是同步返回的，网络慢会卡住小智的对话线程，
 * 而且用户等一下才听到回应，体验差。丢队列后立刻返回"已派发"。
 */
static bool enqueue_dispatch(const std::string& command)
{
    if (_dispatch_queue == nullptr) {
        return false;
    }
    DispatchRequest_t req{};
    const size_t n = std::min(command.size(), sizeof(req.command) - 1);
    std::memcpy(req.command, command.c_str(), n);
    req.command[n] = '\0';
    return xQueueSend(_dispatch_queue, &req, pdMS_TO_TICKS(100)) == pdTRUE;
}

static void dispatch_worker(void*)
{
    DispatchRequest_t req{};
    while (true) {
        if (xQueueReceive(_dispatch_queue, &req, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        mclog::tagInfo(_tag, "dispatching to Hermes: {}", req.command);

        // 借 xiaozhi 的 cJSON 手工拼一个极简 JSON（避免依赖 ArduinoJson）
        cJSON* root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "command", req.command);
        cJSON_AddStringToObject(root, "source", "stackchan");
        char* body = cJSON_PrintUnformatted(root);
        cJSON_Delete(root);

        std::string response;
        const bool ok = body != nullptr && http_post_json("/task", body, response);
        if (body != nullptr) {
            cJSON_free(body);
        }

        if (!ok) {
            mclog::tagWarn(_tag, "dispatch failed");
        } else {
            mclog::tagInfo(_tag, "dispatch ok: {}", response);
        }
    }
}

/* ========================================================================== */
/*                        语音播报 / 屏幕提示                                   */
/* ========================================================================== */

/**
 * 屏幕弹一条通知（P1-⑥：不动语音通道，路过看一眼就知道）
 */
static void show_alert(const std::string& message)
{
    if (message.empty()) {
        return;
    }
    Application::GetInstance().Alert("通知", message.c_str(), "neutral");
}

/* ========================================================================== */
/*                        后台任务：轮询久坐提醒                                */
/* ========================================================================== */

static void remind_with_body(const std::string& message)
{
    mclog::tagInfo(_tag, "reminder: {}", message);

    // 1) LED 变暖橙
    {
        LvglLockGuard lock;
        GetStackChan().leftNeonLight().setColor(0xA0, 0x60, 0x00);
        GetStackChan().rightNeonLight().setColor(0xA0, 0x60, 0x00);
    }

    // 2) 点头 + 转回正前方，像是在叫你
    {
        LvglLockGuard lock;
        auto& motion = GetStackChan().motion();
        motion.pitchServo().moveWithSpeed(20 * 10, 150);
        vTaskDelay(pdMS_TO_TICKS(300));
        motion.pitchServo().moveWithSpeed(0, 150);
        motion.yawServo().moveWithSpeed(0, 150);
    }

    // 3) 提示音（官方自带音效资源）
    hal_bridge::app_play_sound(OGG_NEW_NOTIFICATION);

    // 4) 屏幕显示 + 本地语音播报（v1.9.5：交给 TTS 任务，不阻塞）
    request_speak(message);

    // 5) 3 秒后把灯恢复
    vTaskDelay(pdMS_TO_TICKS(3000));
    {
        LvglLockGuard lock;
        GetStackChan().leftNeonLight().setColor(0, 0, 0);
        GetStackChan().rightNeonLight().setColor(0, 0, 0);
    }
}

static void poll_worker(void*)
{
    std::string response;

    while (true) {
        vTaskDelay(pdMS_TO_TICKS(CONFIG_STACKCHAN_POLL_INTERVAL_SEC * 1000));

        // 小智还没起来或网络没通就跳过
        if (!hal_bridge::is_xiaozhi_ready()) {
            continue;
        }
        if (!http_get("/pending", response)) {
            continue;
        }

        cJSON* root = cJSON_Parse(response.c_str());
        if (root == nullptr) {
            continue;
        }
        const cJSON* items = cJSON_GetObjectItem(root, "items");
        const int count    = cJSON_GetArraySize(items);

        bool handled = false;
        for (int i = 0; i < count; i++) {
            const cJSON* item = cJSON_GetArrayItem(items, i);
            if (item == nullptr) {
                continue;
            }
            const cJSON* type = cJSON_GetObjectItem(item, "type");
            const cJSON* msg  = cJSON_GetObjectItem(item, "message");

            const std::string type_str = (type && type->valuestring) ? type->valuestring : "";
            const std::string message  = (msg && msg->valuestring) ? msg->valuestring : "";

            if (type_str == "remind_standup") {
                // v1.9.12：连续对话模式下设备几乎常绿，死等"灯灭"空闲的话
                // 久坐提醒永远没机会开口。空闲走完整仪式（灯+点头+叮咚+播报）；
                // 监听中退会话插播、播完恢复；说话中不抢，留到下一轮。
                if (hal_bridge::is_xiaozhi_idle()) {
                    remind_with_body(message);
                    handled = true;
                } else if (Application::GetInstance().RequestBroadcastPlayback(message)) {
                    handled = true;
                } else {
                    handled = false;   // 说话中等状态，下一轮再提醒
                    break;
                }
            } else if (type_str == "speak") {
                // 反向通道（P2-⑨）：飞书/日历推来的播报。
                // v1.9.12：不再死等空闲——idle 直接播；监听中退会话插播，
                // 播完恢复监听；说话中不抢，留到下一轮轮询。
                if (Application::GetInstance().RequestBroadcastPlayback(message)) {
                    handled = true;
                } else {
                    handled = false;   // 说话中等状态，下一轮再播
                    break;
                }
            } else if (type_str == "alert") {
                // 只上屏、不出声：路过瞄一眼的通知
                show_alert(message);
                handled = true;
            } else {
                handled = true;
            }
        }
        cJSON_Delete(root);

        if (handled) {
            std::string ack;
            http_post_json("/ack", "{}", ack);
        }
    }
}

/* ========================================================================== */
/*                        后台任务：定时拍照上传                                */
/* ========================================================================== */

/**
 * 2026-09-14 晚 用户报告"有时候拍照后自动重启/卡死"。
 * 桥接日志实锤：19:32:57 拍照 → 设备自动重启（19:33:59 又拍一张，
 * 间隔仅 62s = 重启后 photo_worker 的 60s 首发延时）→ 19:33:59 拍完挂死。
 *
 * 根因分析：JPEG 硬件编码器需要 ~8KB **内部** SRAM（官方 stackchan_camera.cc
 * 注释原文），而对话时音频管线吃掉 ~112KB 内部 RAM，全机内部 RAM 只剩 ~22KB。
 * 拍照撞上对话 = 内部 RAM 告急，分配失败的表现不可控（panic 重启或任务静默死掉）。
 *
 * 修复策略（v1.9.3）：
 * 1. 对话中（!is_xiaozhi_idle）不拍照——人正在跟它说话时本来就在工位，拍了也没意义
 * 2. 内部 RAM 最大连续块低于门槛不拍照——防止 OOM 把系统打挂
 * 3. 每轮拍照前后打内部 RAM 快照，下次再出事，串口日志直接能看到案发时内存
 *
 * v2.5.0 基线重新标定（2026-09-15，栈chan-v250-cam4 日志实测）：
 * - v1.9.x 时代待机内部 RAM ~162KB，48KB 门槛合理；
 * - v2.5.0 上开机 10s 内 ~150KB，第 11s AFE create（WakeNet FD）永久吃 ~22KB，
 *   加上显示/WiFi 全就位后稳态只剩 free≈38KB、largest≈12KB——48KB 门槛从
 *   开机第 12s 起永远过不去，自动拍照被永久拦截（这就是"待机不拍照"的真相）；
 * - 反证：同水位下手动 MCP 拍照全程正常（JPEG 编码器只需 ~8KB 内部 RAM），
 *   说明拍照路径本身在 largest≈12KB 水位是安全的。
 * - 新门槛 8KB：稳态 12KB 能过；真出事时（largest 跌破 8KB）依然拦截。
 *   若未来对话中内存重新吃紧，靠第 1 条（对话中不拍）兜底。
 */

static size_t _internal_free(void)
{
    return heap_caps_get_free_size(MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);
}

static size_t _internal_largest(void)
{
    return heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);
}

/** 拍照门槛：最大连续内部块低于它就跳过本轮（宁可漏拍一次，不让机器挂掉）
 *  v2.5.0 基线稳态 largest≈12KB（48KB 门槛会永久拦截自动拍照，见上方注释） */
/* 内存门槛：JPEG 输出已挪 PSRAM（见 s_jpeg_buf），内部需求大幅下降。
 * 7680B 碎片环境实测放行试拍（2026-09-16），失败路径优雅返回不伤 App */
static constexpr size_t _PHOTO_MIN_INTERNAL_BLOCK = 6 * 1024;

/* ---- 拍摄位（用户 2026-09-16 拍板，Dashboard 可热调，免烧录）------------
 * 语义来源：self.robot.set_head_angles 工具定义（hal_mcp.cpp）：
 *   Yaw -128~+128（负=左，正=右）；Pitch 0~90（90=最抬）。
 * 原则（设计原则）："一定是先转到方向以后再拍照"——转动过程中拍的图是废的。
 * 用 Servo::isMoving() 轮询等到位，4s 超时兜底（宁拍歪不错过本轮）。
 *
 * 免烧录调角度（2026-09-16，产品要求：产品不能让用户烧录）：
 *   生效值 = NVS 持久化值（重启不丢） > 下方编译期默认；
 *   photo_worker 每轮经 /device_config 轮询版本号，桥接 Dashboard 保存的
 *   拍摄位（ver 递增）会在下一个拍照周期自动拉取并写 NVS。 */
static constexpr int _PHOTO_POS_SPEED = 300;

struct PhotoPositions {
    // int32_t：xtensa 上 int32_t=long int，nvs_get_i32 直接要 int32_t*
    int32_t seat_yaw   = 20;   // 人位：右 20°（实拍二次修正，初版 +30/+60 偏了）
    int32_t seat_pitch = 30;   // 人位：抬 30°
    int32_t cup_yaw    = -60;  // 杯位：左 60°
    int32_t cup_pitch  = 10;   // 杯位：抬 10°
    int32_t ver        = 0;    // 桥接侧配置版本号，0 = 从未同步
    // 扩展拍摄位（2026-09-17 多拍摄位，产品决策）：最多 3 个，只拍记录照不做判定。
    // 固件不需要名字——桥接收到 exN 照片后按配置映射回名字。
    static constexpr int kMaxExtras = 3;
    int32_t ex_count   = 0;
    int32_t ex_yaw[kMaxExtras]   = {0, 0, 0};
    int32_t ex_pitch[kMaxExtras] = {0, 0, 0};
};
static PhotoPositions s_photo_pos;

static constexpr const char* _PHOTO_NVS_NS = "photo_pos";

static void _photo_pos_nvs_write()
{
    nvs_handle_t h;
    if (nvs_open(_PHOTO_NVS_NS, NVS_READWRITE, &h) != ESP_OK) {
        mclog::tagWarn(_tag, "photo pos NVS open failed, config not persisted");
        return;
    }
    nvs_set_i32(h, "seat_yaw", s_photo_pos.seat_yaw);
    nvs_set_i32(h, "seat_pitch", s_photo_pos.seat_pitch);
    nvs_set_i32(h, "cup_yaw", s_photo_pos.cup_yaw);
    nvs_set_i32(h, "cup_pitch", s_photo_pos.cup_pitch);
    nvs_set_i32(h, "ver", s_photo_pos.ver);
    nvs_set_i32(h, "ex_cnt", s_photo_pos.ex_count);
    for (int i = 0; i < PhotoPositions::kMaxExtras; ++i) {
        char k[12];
        snprintf(k, sizeof(k), "ex%d_yaw", i);
        nvs_set_i32(h, k, s_photo_pos.ex_yaw[i]);
        snprintf(k, sizeof(k), "ex%d_pitch", i);
        nvs_set_i32(h, k, s_photo_pos.ex_pitch[i]);
    }
    nvs_commit(h);
    nvs_close(h);
}

static void _photo_pos_load()
{
    nvs_handle_t h;
    if (nvs_open(_PHOTO_NVS_NS, NVS_READONLY, &h) != ESP_OK) {
        return;   // 首次启动无记录，用编译期默认
    }
    nvs_get_i32(h, "seat_yaw", &s_photo_pos.seat_yaw);
    nvs_get_i32(h, "seat_pitch", &s_photo_pos.seat_pitch);
    nvs_get_i32(h, "cup_yaw", &s_photo_pos.cup_yaw);
    nvs_get_i32(h, "cup_pitch", &s_photo_pos.cup_pitch);
    nvs_get_i32(h, "ver", &s_photo_pos.ver);
    nvs_get_i32(h, "ex_cnt", &s_photo_pos.ex_count);
    if (s_photo_pos.ex_count < 0 || s_photo_pos.ex_count > PhotoPositions::kMaxExtras) {
        s_photo_pos.ex_count = 0;
    }
    for (int i = 0; i < PhotoPositions::kMaxExtras; ++i) {
        char k[12];
        snprintf(k, sizeof(k), "ex%d_yaw", i);
        nvs_get_i32(h, k, &s_photo_pos.ex_yaw[i]);
        snprintf(k, sizeof(k), "ex%d_pitch", i);
        nvs_get_i32(h, k, &s_photo_pos.ex_pitch[i]);
    }
    nvs_close(h);
    mclog::tagInfo(_tag, "photo pos loaded: seat=({},{}), cup=({},{}), ver={}",
                   s_photo_pos.seat_yaw, s_photo_pos.seat_pitch,
                   s_photo_pos.cup_yaw, s_photo_pos.cup_pitch, s_photo_pos.ver);
}

/* ---- NVS 读写必须离开 PSRAM 栈任务（2026-09-16 崩溃实录）----------------
 * photo_worker 是 PSRAM 栈任务，直接在它里面 nvs commit 会触发
 * `assert failed: spi_flash_disable_interrupts_caches_and_other_cpu
 *  (esp_task_stack_is_sane_cache_disabled())` —— flash 写要关缓存，
 * 关缓存期间 PSRAM 栈不可访问，12:57:45 实机崩溃重启。
 * 方案：运行期的写交给下面这个堆栈任务代劳；启动时的读在 init 上下文
 * （堆栈）做。photo_worker 只改内存 + 发通知，绝不碰 flash。 */
static TaskHandle_t s_photo_nvs_writer = nullptr;
static volatile bool s_photo_pos_dirty = false;

static void photo_nvs_writer_task(void*)
{
    while (true) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        vTaskDelay(pdMS_TO_TICKS(100));   // 合并连续请求
        if (s_photo_pos_dirty) {
            s_photo_pos_dirty = false;
            _photo_pos_nvs_write();       // 堆栈上下文，flash 操作安全
        }
    }
}

/** 每个拍照周期调一次：问桥接有没有新配置（拍摄位 + 空闲休眠）。失败静默，下轮再试。 */
static void _photo_pos_refresh()
{
    /* ---- 稳定性遥测（2026-09-16 排查"自动退出 Agent 模式"）----
     * rst=esp_reset_reason()：本次运行恒定，开机后第一轮上报即是"死因"。
     *   3=PANIC(崩溃) 4/5=看门狗 7=BROWNOUT(电压骤降，无任何日志的那种)
     *   1=POWERON(正常上电) 2=SW(正常重启)。桥接端只在新开机的第一次上报时记录。
     * fi/fb/fp = internal free / internal largest block / PSRAM free：
     *   长时间运行后看曲线是否恶化，验证"内存墙"假设。
     * up = 运行秒数：桥接用来识别"这是新开机"。 */
    char q[192];
    snprintf(q, sizeof(q),
             "?rst=%d&fi=%d&fb=%d&fp=%d&up=%d",
             (int)esp_reset_reason(),
             (int)_internal_free(),
             (int)_internal_largest(),
             (int)heap_caps_get_free_size(MALLOC_CAP_SPIRAM),
             (int)(esp_timer_get_time() / 1000000));
    std::string resp;
    if (!http_get(std::string("/device_config") + q, resp)) {
        return;
    }
    cJSON* root = cJSON_Parse(resp.c_str());
    if (root == nullptr) {
        return;
    }
    const cJSON* ver_j = cJSON_GetObjectItem(root, "ver");
    const cJSON* pos   = cJSON_GetObjectItem(root, "photo_positions");
    const cJSON* seat  = pos ? cJSON_GetObjectItem(pos, "seat") : nullptr;
    const cJSON* cup   = pos ? cJSON_GetObjectItem(pos, "cup") : nullptr;
    const cJSON* sy    = seat ? cJSON_GetObjectItem(seat, "yaw") : nullptr;
    const cJSON* sp    = seat ? cJSON_GetObjectItem(seat, "pitch") : nullptr;
    const cJSON* cy    = cup ? cJSON_GetObjectItem(cup, "yaw") : nullptr;
    const cJSON* cp    = cup ? cJSON_GetObjectItem(cup, "pitch") : nullptr;
    if (ver_j && cJSON_IsNumber(ver_j) && sy && sp && cy && cp
            && cJSON_IsNumber(sy) && cJSON_IsNumber(sp)
            && cJSON_IsNumber(cy) && cJSON_IsNumber(cp)) {
        const int ver = (int)ver_j->valuedouble;
        if (ver > s_photo_pos.ver) {
            s_photo_pos.seat_yaw   = (int)sy->valuedouble;
            s_photo_pos.seat_pitch = (int)sp->valuedouble;
            s_photo_pos.cup_yaw    = (int)cy->valuedouble;
            s_photo_pos.cup_pitch  = (int)cp->valuedouble;

            // 扩展拍摄位（2026-09-17 多拍摄位）：extras=[{name,yaw,pitch}]，最多 3 个
            int ex_cnt = 0;
            const cJSON* extras = cJSON_GetObjectItem(pos, "extras");
            const cJSON* e = extras ? extras->child : nullptr;
            while (e && ex_cnt < PhotoPositions::kMaxExtras) {
                const cJSON* ey = cJSON_GetObjectItem(e, "yaw");
                const cJSON* ep = cJSON_GetObjectItem(e, "pitch");
                if (ey && ep && cJSON_IsNumber(ey) && cJSON_IsNumber(ep)) {
                    s_photo_pos.ex_yaw[ex_cnt]   = (int)ey->valuedouble;
                    s_photo_pos.ex_pitch[ex_cnt] = (int)ep->valuedouble;
                    ++ex_cnt;
                }
                e = e->next;
            }
            s_photo_pos.ex_count = ex_cnt;

            s_photo_pos.ver        = ver;
            s_photo_pos_dirty      = true;
            // NVS 写交给堆栈写入者任务（PSRAM 栈任务直接写 flash 会崩，见上）
            if (s_photo_nvs_writer != nullptr) {
                xTaskNotifyGive(s_photo_nvs_writer);
            }
            mclog::tagInfo(_tag,
                           "photo pos updated to ver={}: seat=({},{}), cup=({},{}), extras={}",
                           ver, s_photo_pos.seat_yaw, s_photo_pos.seat_pitch,
                           s_photo_pos.cup_yaw, s_photo_pos.cup_pitch, ex_cnt);
        }
    }

    /* ---- 空闲休眠热配置（2026-09-16 产品化，产品决策）----
     * {"power_save":{"enabled":bool,"idle_sleep_minutes":int}}
     * enabled=false = 陪伴模式常醒。只在变化时下发，避免每轮都动 timer。 */
    const cJSON* ps = cJSON_GetObjectItem(root, "power_save");
    if (ps) {
        const cJSON* en    = cJSON_GetObjectItem(ps, "enabled");
        const cJSON* mins  = cJSON_GetObjectItem(ps, "idle_sleep_minutes");
        if (en && cJSON_IsBool(en)) {
            static bool s_ps_last_disabled = false;   // 固件默认：不休眠关闭（即休眠开启）
            static int  s_ps_last_minutes  = 5;       // 固件默认 5 分钟
            const bool disabled = !cJSON_IsTrue(en);
            const int minutes   = (mins && cJSON_IsNumber(mins) && mins->valuedouble >= 1)
                                      ? (int)mins->valuedouble : 0;   // 0 = 不改时长
            if (disabled != s_ps_last_disabled
                    || (minutes > 0 && minutes != s_ps_last_minutes)) {
                s_ps_last_disabled = disabled;
                if (minutes > 0) {
                    s_ps_last_minutes = minutes;
                }
                hal_bridge::apply_power_save_hot_config(disabled, minutes);
                mclog::tagInfo(_tag, "power save hot config: disabled={}, minutes={}",
                               disabled, minutes > 0 ? minutes : s_ps_last_minutes);
            }
        }
    }

    cJSON_Delete(root);
}

/* 启动即拉热配置（拍摄位 + 空闲休眠），但必须等网络就绪——
 * 2026-09-16 实录①：init 上下文直接 http_get 会在 tcpip mbox 建好前触发
 * `assert failed: tcpip_send_msg_wait_sem (Invalid mbox)` 进入重启死循环。
 * 2026-09-16 实录②（实机复现）：is_xiaozhi_ready ≠ 网络就绪——这块板子
 * WiFi 初始化排在小智 App 之后，STANDBY 先到、tcpip 后到，赌时序必翻车。
 * 因此这里直接等 STA 网卡拿到 IP（esp_netif 轮询），这才是 HTTP 的真正前提。 */
static bool _sta_network_ready()
{
    esp_netif_t* n = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
    if (n == nullptr) {
        return false;   // STA 网卡还没创建
    }
    esp_netif_ip_info_t info{};
    return esp_netif_get_ip_info(n, &info) == ESP_OK && info.ip.addr != 0;
}

static void deferred_config_pull_task(void*)
{
    for (int i = 0; i < 240; ++i) {   // 最多等 2 分钟拿 IP
        if (_sta_network_ready()) {
            break;
        }
        vTaskDelay(pdMS_TO_TICKS(500));
    }
    vTaskDelay(pdMS_TO_TICKS(1000));   // 拿到 IP 后再稳一下
    _photo_pos_refresh();
    mclog::tagInfo(_tag, "deferred config pull done (boot)");
    vTaskDelete(nullptr);
}

/** 转舵机到指定拍摄位并等到位。舵机动画跑在 LVGL 循环里，操作必须持 LvglLock。 */
static bool _aim_head(int yaw_deg, int pitch_deg)
{
    {
        LvglLockGuard lock;
        auto& motion = GetStackChan().motion();
        motion.yawServo().moveWithSpeed(yaw_deg * 10, _PHOTO_POS_SPEED);
        motion.pitchServo().moveWithSpeed(pitch_deg * 10, _PHOTO_POS_SPEED);
    }
    for (int i = 0; i < 40; ++i) {   // 最多等 4s
        vTaskDelay(pdMS_TO_TICKS(100));
        bool moving = false;
        {
            LvglLockGuard lock;
            auto& motion = GetStackChan().motion();
            moving = motion.yawServo().isMoving() || motion.pitchServo().isMoving();
        }
        if (!moving) {
            mclog::tagInfo(_tag, "head aimed: yaw={} pitch={}", yaw_deg, pitch_deg);
            return true;
        }
    }
    mclog::tagWarn(_tag, "head aim timeout: yaw={} pitch={}, continue anyway", yaw_deg, pitch_deg);
    return true;   // 超时也继续拍，本轮不算失败
}

static void photo_worker(void*)
{
    constexpr int interval_min = CONFIG_STACKCHAN_PHOTO_INTERVAL_MIN;
    if (interval_min <= 0) {
        // 关闭自动拍照：不启动任务，避免无谓耗电
        mclog::tagInfo(_tag, "auto photo disabled");
        vTaskDelete(nullptr);
        return;
    }

    std::string answer;
    bool seat_ok = false;

    // 拍摄位已在 init 上下文从 NVS 载入（PSRAM 栈任务不能碰 flash，见上）

    while (true) {
        // 错开启动时间，避免刚开机就和 WiFi 抢资源
        vTaskDelay(pdMS_TO_TICKS(60 * 1000));

        if (!hal_bridge::is_xiaozhi_ready()) {
            continue;
        }

        // ① 对话中不拍：人在说话就说明在工位
        if (!hal_bridge::is_xiaozhi_idle()) {
            mclog::tagInfo(_tag, "photo skipped: conversation active");
            vTaskDelay(pdMS_TO_TICKS(interval_min * 60 * 1000));
            continue;
        }

        /* ② 拍照前拆卸音频管线（方案 A，2026-09-16 落地）：
         * 小智 App 开着时唤醒词引擎+音频输入占着内部 RAM，内存碎片化后
         * 相机一开就把 App 挤死（实录：15:55:53 拍照轮开始 → 无照片上传 →
         * App 退回桌面）。先停唤醒词+关输入，腾出内存再拍，拍完无条件恢复。
         * 只在 idle 状态拆，用户正在说话的时刻到不了这里。 */
        auto& audio_service = Application::GetInstance().GetAudioService();
        const bool wwd_was_running = audio_service.IsWakeWordRunning();
        if (wwd_was_running) {
            audio_service.EnableWakeWordDetection(false);
            vTaskDelay(pdMS_TO_TICKS(150));
        }
        auto codec = Board::GetInstance().GetAudioCodec();
        if (codec) {
            codec->EnableInput(false);
        }
        vTaskDelay(pdMS_TO_TICKS(150));   // 等音频管线释放缓冲

        // ②b 竞态防护（2026-09-16 实录）：拆卸的这几百毫秒里用户可能刚喊了
        // 唤醒词——设备已进对话但我们手里是拆掉的状态。整轮放弃立即恢复，
        // 否则对话头 15 秒没麦克风（"唤醒了没法对话"），恢复还会把唤醒词
        // 塞回监听态（IN_LISTENING 双管线老毛病）。
        if (!hal_bridge::is_xiaozhi_idle()) {
            mclog::tagInfo(_tag, "photo aborted: conversation started during teardown");
            if (codec) {
                codec->EnableInput(true);
            }
            // 唤醒词不在这里补开：对话状态机自己管理（进监听态会按配置开/关），
            // 拍照任务在对话中重开唤醒词 = 双管线并存老毛病的触发器
            vTaskDelay(pdMS_TO_TICKS(interval_min * 60 * 1000));
            continue;
        }

        // ③ 内存门槛复测（拆卸后测才作数）：连续内部块不够就恢复后跳过
        if (_internal_largest() < _PHOTO_MIN_INTERNAL_BLOCK) {
            mclog::tagWarn(_tag,
                           "photo skipped: internal largest={}B free={}B below {}B",
                           (int)_internal_largest(), (int)_internal_free(),
                           (int)_PHOTO_MIN_INTERNAL_BLOCK);
            if (codec) {
                codec->EnableInput(true);
            }
            if (wwd_was_running && hal_bridge::is_xiaozhi_idle()) {
                audio_service.EnableWakeWordDetection(true);
            }
            vTaskDelay(pdMS_TO_TICKS(interval_min * 60 * 1000));
            continue;
        }

        // ④ 人位：先转到位，再拍在座判定照（先问桥接有没有新拍摄位配置）
        _photo_pos_refresh();
        mclog::tagInfo(_tag, "photo start: internal largest={}B free={}B",
                       (int)_internal_largest(), (int)_internal_free());
        _aim_head(s_photo_pos.seat_yaw, s_photo_pos.seat_pitch);
        seat_ok = upload_photo("现在主人在工位吗？请判断画面里有没有人。", answer, "seat");

        // ⑤ 水杯位：再转到水杯方向，拍水位判定照（人位拍失败也照拍，互不影响）
        _aim_head(s_photo_pos.cup_yaw, s_photo_pos.cup_pitch);
        if (_internal_largest() >= _PHOTO_MIN_INTERNAL_BLOCK) {
            upload_photo("这张照片对着桌上的水杯，请判断水位。", answer, "cup");
        } else {
            mclog::tagWarn(_tag, "cup photo skipped: low internal memory");
        }

        // ⑤b 扩展拍摄位（2026-09-17 多拍摄位，产品决策）：逐个转向拍记录照。
        // 只存档不判定（桥接侧不调云 API），每个位拍照前都复测内存门槛。
        for (int i = 0; i < s_photo_pos.ex_count; ++i) {
            if (_internal_largest() < _PHOTO_MIN_INTERNAL_BLOCK) {
                mclog::tagWarn(_tag,
                               "extra photo {} skipped: internal largest={}B",
                               i + 1, (int)_internal_largest());
                break;
            }
            _aim_head(s_photo_pos.ex_yaw[i], s_photo_pos.ex_pitch[i]);
            char tag[8];
            snprintf(tag, sizeof(tag), "ex%d", i + 1);
            upload_photo("常规记录照", answer, tag);
        }

        // ⑥ 立刻恢复音频管线，再回正（恢复优先于一切收尾动作）。
        // 唤醒词只在 idle 才补开——拍照期间用户喊了唤醒词的话，设备已进
        // 对话状态，对话状态机会自己管理唤醒词；这里硬塞回去就是
        // "监听态双 AFE 并存"老毛病（2026-09-16 串口实录：监听态里
        // "Wake word detected state=5"→AbortSpeaking 吃掉用户语音）。
        if (codec) {
            codec->EnableInput(true);
        }
        if (wwd_was_running && hal_bridge::is_xiaozhi_idle()) {
            audio_service.EnableWakeWordDetection(true);
        }
        _aim_head(0, 0);

        if (seat_ok) {
            mclog::tagInfo(_tag, "photo round done: internal largest={}B free={}B",
                           (int)_internal_largest(), (int)_internal_free());
            // 拍完立刻再睡满整个间隔
            vTaskDelay(pdMS_TO_TICKS((interval_min - 1) * 60 * 1000));
        } else {
            mclog::tagWarn(_tag, "photo failed: internal largest={}B free={}B",
                           (int)_internal_largest(), (int)_internal_free());
            // 失败就等一轮再来，别循环猛打
            vTaskDelay(pdMS_TO_TICKS(interval_min * 60 * 1000));
        }
    }
}

/* ========================================================================== */
/*                              MCP 工具注册                                   */
/* ========================================================================== */

static void register_mcp_tools()
{
    auto& mcp_server = McpServer::GetInstance();

    // ---- 把任意自然语言任务交给本机 Hermes ----
    mclog::tagInfo(_tag, "add hermes.dispatch tool");
    mcp_server.AddTool(
        "self.hermes.dispatch",
        "把用户的请求交给本机 Hermes 智能体执行。适用于：整理工作日志、生成日报/周报、"
        "整理会议纪要、查询飞书日程或文档、查询公司财务数据等需要调用电脑上工具的任务。"
        "调用后立即返回，任务在后台执行，长结果会直接推送到用户的飞书，"
        "请不要把长内容念出来，只播报一句简短的确认。",
        PropertyList({Property("command", kPropertyTypeString, std::string(""))}),
        [](const PropertyList& properties) -> ReturnValue {
            const std::string command = properties["command"].value<std::string>();
            if (command.empty()) {
                return std::string("没有听清要做什么，再说一遍好吗？");
            }
            mclog::tagInfo(_tag, "dispatch command: {}", command);
            if (!enqueue_dispatch(command)) {
                return std::string("派发失败，队列忙不过来，稍后再试。");
            }
            return std::string("好的，已经交给 Hermes 处理，结果稍后推送到你的飞书。");
        });

    // ---- 工作日志（给 LLM 一个明确的短语入口，提高命中率）----
    mclog::tagInfo(_tag, "add hermes.work_log tool");
    mcp_server.AddTool("self.hermes.work_log",
                       "让 Hermes 整理今天的工作日志并推送到飞书。"
                       "当用户说\"整理今天的工作日志\"\"写个工作日志\"时调用。"
                       "可选参数日期说明，例如\"昨天\"\"本周\"，留空表示今天。",
                       PropertyList({Property("when", kPropertyTypeString, std::string("今天"))}),
                       [](const PropertyList& properties) -> ReturnValue {
                           const std::string when = properties["when"].value<std::string>();
                           const std::string cmd =
                               "请整理" + (when.empty() ? std::string("今天") : when) +
                               "的工作日志，汇总到飞书文档或消息中推送给主人。";
                           if (!enqueue_dispatch(cmd)) {
                               return std::string("派发失败，稍后再试。");
                           }
                           return std::string("好的，正在整理，稍后推到你的飞书。");
                       });

    // ---- 会议纪要 ----
    mclog::tagInfo(_tag, "add hermes.meeting_summary tool");
    mcp_server.AddTool("self.hermes.meeting_summary",
                       "让 Hermes 整理会议纪要并推送到飞书。"
                       "当用户说\"整理会议纪要\"\"把刚才的会议总结一下\"时调用。",
                       PropertyList({Property("topic", kPropertyTypeString, std::string(""))}),
                       [](const PropertyList& properties) -> ReturnValue {
                           const std::string topic = properties["topic"].value<std::string>();
                           std::string cmd = "请整理最近的会议纪要";
                           if (!topic.empty()) {
                               cmd += "（主题：" + topic + "）";
                           }
                           cmd += "，并推送到主人的飞书。";
                           if (!enqueue_dispatch(cmd)) {
                               return std::string("派发失败，稍后再试。");
                           }
                           return std::string("好的，正在整理会议纪要，稍后推到你的飞书。");
                       });

    // ---- 用嘴暂停提醒（P1-⑦ / 会议模式 P2-⑪）----
    mclog::tagInfo(_tag, "add hermes.pause_reminders tool");
    mcp_server.AddTool(
        "self.hermes.pause_reminders",
        "暂停久坐提醒和日程播报，让机器人安静一会儿。"
        "当用户说\"我要开会\"\"暂停提醒\"\"别打扰我\"时调用。"
        "minutes 为暂停多少分钟，到点自动恢复；开会场景建议 60，不填默认 60。",
        PropertyList({Property("minutes", kPropertyTypeInteger, 60, 1, 720)}),
        [](const PropertyList& properties) -> ReturnValue {
            const int minutes = properties["minutes"].value<int>();
            if (minutes <= 0 || minutes > 720) {
                return std::string("暂停时长要在 1 到 720 分钟之间。");
            }
            cJSON* root = cJSON_CreateObject();
            cJSON_AddBoolToObject(root, "enabled", false);
            cJSON_AddNumberToObject(root, "minutes", minutes);
            char* body = cJSON_PrintUnformatted(root);
            cJSON_Delete(root);
            std::string response;
            const bool ok = (body != nullptr) && http_post_json("/pause", body, response);
            if (body != nullptr) {
                cJSON_free(body);
            }
            if (!ok) {
                return std::string("暂停失败了，回头再试一次。");
            }
            if (minutes >= 60) {
                return std::string("好的，提醒已暂停 ") + std::to_string(minutes / 60) +
                       " 小时" + (minutes % 60 ? (" " + std::to_string(minutes % 60) + " 分钟") : "") +
                       "，到点自动恢复。";
            }
            return std::string("好的，提醒已暂停 ") + std::to_string(minutes) +
                   " 分钟，到点自动恢复。";
        });

    // ---- 用嘴恢复提醒 ----
    mclog::tagInfo(_tag, "add hermes.resume_reminders tool");
    mcp_server.AddTool(
        "self.hermes.resume_reminders",
        "恢复久坐提醒和日程播报。当用户说\"恢复提醒\"\"开会结束了\"时调用。",
        PropertyList(std::vector<Property>{}),
        [](const PropertyList&) -> ReturnValue {
            std::string response;
            if (!http_post_json("/pause", "{\"enabled\": true}", response)) {
                return std::string("恢复失败了，回头再试一次。");
            }
            return std::string("好的，提醒已经恢复。");
        });

    // ---- 用嘴退出对话（v1.9.10）----
    // 用户说"你退下吧"时，LLM 调用此工具关闭音频通道进入待机。
    // 必须在服务器 goodbye 抢先断开之前由 LLM 主动调用，配合固件里的
    // user_exit_requested_ 标记，通道关闭后不会被自动重连拉起来。
    mclog::tagInfo(_tag, "add hermes.exit_conversation tool");
    mcp_server.AddTool(
        "self.hermes.exit_conversation",
        "结束当前语音对话，让机器人进入待机（灯灭）。"
        "当用户说\"你退下吧\"\"退下\"\"休息吧\"\"别聊了\"\"你去忙吧\"时调用。"
        "调用后不要继续说话，直接结束回复。",
        PropertyList(std::vector<Property>{}),
        [](const PropertyList&) -> ReturnValue {
            Application::GetInstance().RequestUserExit();
            return std::string("好的，我退下了。");
        });
}

/* ========================================================================== */
/*                                   入口                                      */
/* ========================================================================== */

// 2026-09-14：对话期间内部 RAM 会跌到 ~5KB（音频管线吃掉 ~112KB），
// 我们 3 个任务共 22KB 内部栈是压垮音频管线的最后稻草（唤醒词检测任务 4KB
// 分配失败会静默死掉，机器人就"叫不醒、没声音"）。
// 因此把 HAL 任务栈全部挪到 PSRAM（8MB），需要 sdkconfig 里
// CONFIG_SPIRAM_ALLOW_STACK_EXTERNAL_MEMORY=y（已开启）。
// 用法与 xiaozhi-esp32 afe_wake_word.cc 的 encode 任务一致：
// 栈放 PSRAM，StaticTask_t 控制块放内部 RAM。
static TaskHandle_t _create_psram_task(TaskFunction_t fn, const char* name,
                                       size_t stack_bytes, UBaseType_t prio)
{
    auto* stack = (StackType_t*)heap_caps_malloc(stack_bytes, MALLOC_CAP_SPIRAM);
    auto* tcb   = (StaticTask_t*)heap_caps_malloc(sizeof(StaticTask_t), MALLOC_CAP_INTERNAL);
    if (stack == nullptr || tcb == nullptr) {
        mclog::tagError(_tag, "psram task {} alloc failed", name);
        if (stack) heap_caps_free(stack);
        if (tcb) heap_caps_free(tcb);
        return nullptr;
    }
    TaskHandle_t handle = xTaskCreateStatic(fn, name, stack_bytes, nullptr,
                                            prio, stack, tcb);
    if (handle == nullptr) {
        mclog::tagError(_tag, "psram task {} create failed", name);
        heap_caps_free(stack);
        heap_caps_free(tcb);
    }
    return handle;
}

void init()
{
    mclog::tagInfo(_tag, "init, bridge = {}", _base_url());

    // 开机原因（v1.9.3 排障增强）：panic=软件崩溃重启 / SW=手动或程序重启 /
    // BROWNOUT=电压跌落（供电不稳）。出问题时这行日志就是第一现场。
    const int reset_reason = (int)esp_reset_reason();
    mclog::tagInfo(_tag, "reset reason: {} (0=power-on 1=SW 4=panic 9=brownout)", reset_reason);

    register_mcp_tools();

    _dispatch_queue = xQueueCreate(4, sizeof(DispatchRequest_t));
    if (_dispatch_queue == nullptr) {
        mclog::tagError(_tag, "failed to create dispatch queue");
    } else {
        // HTTP 请求需要较大栈，网络栈在 8KB 左右比较稳
        // 2026-09-14：改 PSRAM 栈，省出 8KB 内部 RAM 给音频管线
        _create_psram_task(dispatch_worker, "hermes_dispatch", 8192, 4);
    }

    // 2026-09-14：同样改 PSRAM 栈，省出 14KB 内部 RAM
    // 拍摄位：init 上下文（堆栈）读 NVS 安全；photo_worker 等 PSRAM 栈任务
    // 绝不碰 flash（写走 photo_nvs_writer_task，见 photo-pos 块内注释）
    _photo_pos_load();
    if (xTaskCreate(photo_nvs_writer_task, "hermes_nvsw", 3072, nullptr, 2,
                    &s_photo_nvs_writer) != pdPASS) {
        mclog::tagError(_tag, "failed to create photo nvs writer task");
    }
    // 启动即拉一次热配置（拍摄位 + 空闲休眠），不等第一个拍照周期。
    // 不能在 init 上下文直接 HTTP（tcpip 未就绪会 assert 死循环，见任务函数注释），
    // 放到等网络就绪的延迟任务里做（普通堆栈，NVS 写走 writer 任务，安全）
    if (xTaskCreate(deferred_config_pull_task, "hermes_cfgpull", 6144, nullptr, 3, nullptr) != pdPASS) {
        mclog::tagError(_tag, "failed to create deferred config pull task");
    }
    _create_psram_task(poll_worker, "hermes_poll", 6144, 3);
    _create_psram_task(photo_worker, "hermes_photo", 8192, 3);

    // v1.9.5：TTS 专用任务——语音下载/播放的慢和阻塞都关在这里面，
    // 轮询、派单、拍照完全不受影响
    _tts_queue = xQueueCreate(4, sizeof(TtsRequest_t));
    if (_tts_queue == nullptr) {
        mclog::tagError(_tag, "failed to create tts queue");
    } else {
        _create_psram_task(tts_worker, "hermes_tts", 8192, 3);
        // v1.9.12：把自己注册为小智的"播报播放器"。监听中收到桥接播报时，
        // 小智会先退会话再回来调 request_speak，播完自动恢复连续对话。
        Application::GetInstance().SetBroadcastPlayer(
            [](const std::string& text) -> bool { return request_speak(text); });
    }

    // ⚠️ 2026-09-14 深夜实测：SetPowerSaveLevel(PERFORMANCE) 会让内部 RAM
    // 从 144KB 掉到 45KB（WiFi 常驻高功耗模式的缓冲代价），挤压唤醒词与
    // 音频管线，得不偿失。撤掉，恢复默认省电模式。大文件传输卡顿问题
    // 随本地 TTS 一起下线（不再有 70KB 下载），小包轮询在省电模式下够用。
    // Board::GetInstance().SetPowerSaveLevel(PowerSaveLevel::PERFORMANCE);

    mclog::tagInfo(_tag, "init done");
}

}  // namespace hal_hermes
