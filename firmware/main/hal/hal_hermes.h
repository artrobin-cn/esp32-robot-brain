/*
 * SPDX-FileCopyrightText: 2026 Robin / StackChan 改造项目
 * SPDX-License-Identifier: MIT
 *
 * hal_hermes —— StackChan 与本机 Hermes Agent / 桥接服务的对接层
 *
 * 提供三件事：
 *   1. 注册 self.hermes.* MCP 工具，让云端 LLM 能"派活"给本机 Hermes
 *   2. 后台任务：轮询桥接服务的 /pending，把久坐提醒变成动作 + 灯光 + 音效
 *   3. 后台任务：定时拍照上传（复用官方 JPEG 编码与 multipart 逻辑）
 *
 * 集成方式见同目录《集成说明.md》
 */
#pragma once

namespace hal_hermes {

/**
 * @brief 初始化：注册 MCP 工具 + 启动后台任务
 *
 * 应该在 Hal::init() 里、xiaozhi_mcp_init() 之后调用。
 * 此时 NVS / I2C / 舵机 / 摄像头句柄均已就绪。
 * 网络与小智运行时可能还没起来，后台任务内部会自行等待。
 */
void init();

}  // namespace hal_hermes
