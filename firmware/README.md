
## 本仓库定制说明（StackChan Work Secretary）

本固件是 **ESP32 Robot Brain** 项目的设备端，运行在 M5Stack CoreS3 + Stack-chan 机体上，与仓库根目录的桥接服务（`bridge_server.py`）配合使用：

- `main/hal/`：定制 HAL（`hal_hermes` 拍摄位调度/上传、`hal_bridge` 热配置、`hal_ble` 配网等），照片按周期 POST 到桥接 `/vision`，并轮询 `/device_config` 拉取免烧录热配置；
- `patches/`：对上游 xiaozhi-esp32 的集成补丁（含异步语音通知协议）；
- `components/esp-ml307/`：**本地组件覆盖**（ESP-IDF 同名本地组件优先于 managed 组件），包含 TcpClient 析构竞态修复（`receive_task_started_` 守卫），修复"拍照后随机 PANIC"崩溃——上游 78/esp-ml307 尚未合并该修复；
- 其余按 `repos.json` 由 `fetch_repos.py` 拉取上游依赖后自动打补丁。

### 快速开始

```bash
# 1) 拉取上游依赖（xiaozhi-esp32 v2.2.4 等，见 repos.json）
python3 ./fetch_repos.py

# 2) 环境：ESP-IDF v6.0.3（本项目实测版本）
source ~/esp/esp-idf-v6.0.3/export.sh

# 3) 编译烧录（M5Stack CoreS3）
idf.py -B build set-target esp32s3
idf.py -B build build flash monitor

# 4) 首次配网：设备热点/蓝牙配网填 WiFi；
#    桥接地址默认 http://192.168.1.100:8787，改成本机局域网 IP；
#    device_token 与桥接 config.yaml 的 security.device_token 一致。
```

### 许可与致谢

- 本目录基础框架来自 M5Stack StackChan 开源仓库（MIT License, © M5Stack Technology）；
- 设备端应用基于 [78/xiaozhi-esp32](https://github.com/78/xiaozhi-esp32) v2.2.4（MIT License, © Shenzhen Xinzhi Future Technology）；
- `components/esp-ml307` 来自 [78/esp-ml307](https://github.com/78/esp-ml307)（Apache License 2.0），含本地竞态修复；
- 上层业务定制代码（`main/hal/`、`patches/`）以本仓库根目录的 MIT 许可发布。

---

## Build

### Fetch Dependencies

```bash
python3 ./fetch_repos.py
```

### Tool Chains

[ESP-IDF v5.5.4](https://docs.espressif.com/projects/esp-idf/en/v5.5.4/esp32s3/index.html)

### Build

```bash
idf.py build
```

### Host-side tests

The motion coordinate helpers can be tested without ESP-IDF hardware:

```bash
cmake -S tests -B build-host-tests
cmake --build build-host-tests
ctest --test-dir build-host-tests --output-on-failure
```

### Flash

```bash
idf.py flash
```
