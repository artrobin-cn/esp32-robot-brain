# The following five lines of boilerplate have to be in your project's
# CMakeLists in this exact order for cmake to work correctly
cmake_minimum_required(VERSION 3.16)

# [StackChan 改造] 必须高于官方版本号！
# 官方 OTA 用点分段数字严格比大小，本地版本 <= 服务器版本就会被静默覆盖。
# 官方 v1.5.1 → 这里提到 1.9.0（只写纯数字和点，加字母会让 std::stoi 抛异常）。
set(PROJECT_VER "1.9.12")
add_definitions(-DFIRMWARE_VERSION=\"${PROJECT_VER}\")

# Add this line to disable the specific warning
add_compile_options(-Wno-missing-field-initializers)

# Auto-load a local sdkconfig overlay (git-ignored) when present. Lets
# self-hosted / custom deployments pin CONFIG_STACKCHAN_SERVER_URL,
# CONFIG_OTA_URL, etc. without touching committed defaults.
# Manual equivalent: SDKCONFIG_DEFAULTS="sdkconfig.defaults;sdkconfig.defaults.local" idf.py build
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/sdkconfig.defaults.local")
    set(SDKCONFIG_DEFAULTS
        "${CMAKE_CURRENT_SOURCE_DIR}/sdkconfig.defaults;${CMAKE_CURRENT_SOURCE_DIR}/sdkconfig.defaults.local"
        CACHE STRING "ESP-IDF sdkconfig default files (auto-extended with local overlay)" FORCE)
    message(STATUS "StackChan: detected sdkconfig.defaults.local, applying overlay")
endif()

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(stack-chan)
