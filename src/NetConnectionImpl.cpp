// NetConnectionImpl.cpp - 连接实现

#include <AYNetwork.h>

namespace ayt::net
{

class NetConnectionImpl : public NetConnection {
public:
    uint32_t getId() const override { return _id; }
    uint32_t getHostId() const override { return _hostId; }
    const char* getAddress() const override { return _address.c_str(); }
    bool isConnected() const override { return _connected; }
    int getPing() const override { return _ping; }

    void send(uint8_t channel, const void* data, size_t size) override {
        // TODO: 实现
    }

    void disconnect(const char* reason = nullptr) override {
        // TODO: 实现
    }

    void setUserData(void* data) override { _userData = data; }
    void* getUserData() const override { return _userData; }

private:
    uint32_t _id = 0;
    uint32_t _hostId = 0;
    std::string _address;
    bool _connected = false;
    int _ping = 0;
    void* _userData = nullptr;
};

} // namespace ayt::net