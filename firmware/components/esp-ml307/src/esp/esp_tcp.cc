#include "esp_tcp.h"

#include <esp_log.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

static const char *TAG = "EspTcp";

EspTcp::EspTcp() {
    event_group_ = xEventGroupCreate();
}

EspTcp::~EspTcp() {
    Disconnect();

    // 修复：接收任务可能仍在执行断开回调，析构前必须等它退出（StackChan 2026-09-17 竞态补丁）
    if (receive_task_started_ && event_group_ != nullptr) {
        xEventGroupWaitBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT, pdFALSE, pdFALSE, pdMS_TO_TICKS(10000));
    }

    if (event_group_ != nullptr) {
        vEventGroupDelete(event_group_);
        event_group_ = nullptr;
    }
}

NetworkResult<> EspTcp::Connect(const std::string& host, int port) {
    // 确保先断开已有连接
    if (connected_) {
        Disconnect();
    }

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    // host is domain
    struct hostent *server = gethostbyname(host.c_str());
    if (server == NULL) {
        ESP_LOGE(TAG, "Failed to get host by name");
        return Fail(NetworkError::FromHErrno(h_errno));
    }
    memcpy(&server_addr.sin_addr, server->h_addr, server->h_length);
    ESP_LOGI(TAG, "Resolved %s -> %s", host.c_str(), inet_ntoa(*(struct in_addr *)server->h_addr));

    tcp_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd_ < 0) {
        ESP_LOGE(TAG, "Failed to create socket");
        return Fail(NetworkError::FromErrno(errno));
    }

    int ret = connect(tcp_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (ret < 0) {
        auto err = NetworkError::FromErrno(errno);
        ESP_LOGE(TAG, "Failed to connect to %s:%d: %s", host.c_str(), port, err.ToString().c_str());
        close(tcp_fd_);
        tcp_fd_ = -1;
        return Fail(err);
    }

    connected_ = true;
    receive_task_started_ = true;

    xEventGroupClearBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
    xTaskCreate([](void* arg) {
        EspTcp* tcp = (EspTcp*)arg;
        tcp->ReceiveTask();
        xEventGroupSetBits(tcp->event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
        vTaskDelete(NULL);
    }, "tcp_receive", 4096, this, 1, &receive_task_handle_);
    return {};
}

void EspTcp::Disconnect() {
    // 如果已经断开，直接返回
    if (!connected_) {
        // 修复（StackChan 2026-09-17）：被动断开后 fd 已关，但接收任务可能仍在执行
        // 断开回调（OnTcpDisconnected → notify_all），析构前必须等它退出
        if (receive_task_started_ && event_group_ != nullptr) {
            xEventGroupWaitBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT, pdFALSE, pdFALSE, pdMS_TO_TICKS(10000));
        }
        return;
    }

    // 主动断开，需要等待接收任务退出
    DoDisconnect(true);
}

void EspTcp::DoDisconnect(bool wait_for_task) {
    connected_ = false;

    if (tcp_fd_ != -1) {
        close(tcp_fd_);
        tcp_fd_ = -1;
    }

    // 修复（StackChan 2026-09-17）：等待逻辑不能依赖 fd 状态——
    // 被动断开时 fd 已被接收任务关闭，原写法会让析构路径跳过等待，
    // 导致对象被销毁而接收任务仍在跑回调（use-after-free 竞态）。
    // 被动断开（wait_for_task=false）时当前就是接收任务，无需等待。
    if (wait_for_task && receive_task_started_) {
        auto bits = xEventGroupWaitBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT, pdFALSE, pdFALSE, pdMS_TO_TICKS(10000));
        if (!(bits & ESP_TCP_EVENT_RECEIVE_TASK_EXIT)) {
            ESP_LOGE(TAG, "Failed to wait for receive task exit");
        }
    }

    // 断开连接时触发断开回调
    if (disconnect_callback_) {
        disconnect_callback_();
    }
}

int EspTcp::Send(const std::string& data) {
    if (!connected_) {
        ESP_LOGE(TAG, "Not connected");
        return -1;
    }

    size_t total_sent = 0;
    size_t data_size = data.size();
    const char* data_ptr = data.data();

    while (total_sent < data_size) {
        int ret = send(tcp_fd_, data_ptr + total_sent, data_size - total_sent, 0);

        if (ret <= 0) {
            ESP_LOGE(TAG, "Send failed: ret=%d, errno=%d", ret, errno);
            return ret;
        }

        total_sent += ret;
    }

    return total_sent;
}

void EspTcp::ReceiveTask() {
    std::string data;
    while (connected_) {
        data.resize(1500);
        int ret = recv(tcp_fd_, data.data(), data.size(), 0);
        if (ret <= 0) {
            if (ret < 0) {
                ESP_LOGE(TAG, "TCP receive failed: %d", ret);
            }
            // 被动断开，不需要等待接收任务退出（当前就是接收任务）
            DoDisconnect(false);
            break;
        }

        if (stream_callback_) {
            data.resize(ret);
            stream_callback_(data);
        }
    }
}

