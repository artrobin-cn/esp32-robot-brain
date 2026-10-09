/*
 * StackChan patch (2026-09-15)
 *
 * ESP-IDF 6 下，esp-sr 2.4.7 等预编译库在 Espressif 官方构建时启用了
 * -Wl,--wrap=esp_log_write 与 -Wl,--wrap=esp_log_writev，其目标文件里对
 * 这两个函数的引用被记录为 __wrap_esp_log_write(__v)。而 IDF 6 的 log
 * 组件不提供这些符号，链接时需同时满足：
 *   1. 链接选项 -Wl,--wrap=esp_log_write -Wl,--wrap=esp_log_writev
 *      （在 main/CMakeLists.txt 中添加）；
 *   2. 提供本文件的 __wrap_ 实现，转发到 __real_ 版本（即 IDF 原函数）。
 *
 * 注意：本文件内部必须调用 __real_esp_log_writev 而非 esp_log_writev，
 * 否则会因 wrap 重定向造成无限递归。
 */
#include <stdarg.h>

#include "esp_log_level.h"

void __real_esp_log_writev(esp_log_level_t level, const char *tag, const char *format, va_list args);

void __wrap_esp_log_writev(esp_log_level_t level, const char *tag, const char *format, va_list args)
{
    __real_esp_log_writev(level, tag, format, args);
}

void __wrap_esp_log_write(esp_log_level_t level, const char *tag, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    __real_esp_log_writev(level, tag, format, args);
    va_end(args);
}
