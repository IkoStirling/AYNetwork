// AYTest_RepNotify.cpp - R4.1-B per-field RepNotify callback tests

#include <AYNetwork.h>
#include <AYTest.h>

#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/detail/ReflectImpl.h>
#include <AYReflect.h>

#include <atomic>
#include <string>
#include <vector>

using namespace ayt::net;

namespace
{

struct RepNotifyObj {
    int32_t score = 0;
    int32_t lives = 3;
};

struct RepNotifyRegistrar {
    RepNotifyRegistrar() {
        using ayt::reflect::FieldInfoImpl;
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        auto& reg = TypeRegistryImpl::instance();
        if (!reg.findType("RepNotifyObj")) {
            auto* info = new TypeInfoImpl<RepNotifyObj>(
                "RepNotifyObj",
                ayt::reflect::detail::defaultCreate<RepNotifyObj>,
                ayt::reflect::detail::defaultDestroy<RepNotifyObj>,
                ayt::reflect::detail::defaultCopy<RepNotifyObj>);
            using T = RepNotifyObj;
            info->addField(new FieldInfoImpl(
                "score", reg.findType<int32_t>(), offsetof(T, score),
                ayt::reflect::FieldAttribute::Serialize
                    | ayt::reflect::FieldAttribute::NetReplicate
                    | ayt::reflect::FieldAttribute::RepNotify));
            info->addField(new FieldInfoImpl(
                "lives", reg.findType<int32_t>(), offsetof(T, lives),
                ayt::reflect::FieldAttribute::Serialize
                    | ayt::reflect::FieldAttribute::NetReplicate));
            reg.registerTypeInfo("RepNotifyObj", info);
        }
    }
};
static RepNotifyRegistrar g_repNotifyRegistrar;

class RepNotifyRecordingExtension : public INetworkExtension {
public:
    struct NotifyEvent {
        uint32_t netId = 0;
        std::string fieldName;
    };

    std::vector<NotifyEvent> events;

    void onRepNotify(void*, const ayt::reflect::ITypeInfo*, uint32_t netId,
                     const char* fieldName) override {
        events.push_back({netId, fieldName ? fieldName : ""});
    }
};

BitStream makeReplicationBody(const ayt::reflect::ITypeInfo* type, void* obj, uint32_t netId) {
    BitStream body;
    CHECK(ReflectSerializer::serializeObject(type, obj, netId, body));
    body.resetForRead();
    return body;
}

} // anonymous namespace

TEST_SUITE(RepNotify)

TEST_CASE(RepNotifyFieldFiresOnReceive) {
    ayt::test::setCurrentCase("RepNotifyFieldFiresOnReceive");

    ReplicationManager mgr(nullptr);
    RepNotifyRecordingExtension ext;
    mgr.setExtension(&ext);

    RepNotifyObj serverObj;
    serverObj.score = 99;
    serverObj.lives = 1;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("RepNotifyObj");
    CHECK(type != nullptr);

    RepNotifyObj clientObj;
    mgr.registerObject(&clientObj, type, 77);

    BitStream body = makeReplicationBody(type, &serverObj, 77);
    CHECK(mgr.onReceive(kMsgTypeReplication, body, nullptr));

    CHECK_INT_EQ(clientObj.score, 99);
    CHECK_INT_EQ(clientObj.lives, 1);
    CHECK_INT_EQ(static_cast<int>(ext.events.size()), 1);
    if (ext.events.size() >= 1) {
        CHECK(ext.events[0].netId == 77u);
        CHECK(ext.events[0].fieldName == "score");
    }
}

TEST_CASE(NonRepNotifyFieldDoesNotFire) {
    ayt::test::setCurrentCase("NonRepNotifyFieldDoesNotFire");

    ReplicationManager mgr(nullptr);
    RepNotifyRecordingExtension ext;
    mgr.setExtension(&ext);

    RepNotifyObj serverObj;
    serverObj.score = 0;
    serverObj.lives = 5;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("RepNotifyObj");
    CHECK(type != nullptr);

    RepNotifyObj clientObj;
    mgr.registerObject(&clientObj, type, 88);

    BitStream body;
    std::vector<uint32_t> dirty{1}; // dense index 1 = "lives"
    ReflectSerializer::serializeDirtyFields(type, &serverObj, 88, dirty, body);
    body.resetForRead();

    CHECK(mgr.onReceive(kMsgTypeDelta, body, nullptr));

    CHECK_INT_EQ(clientObj.lives, 5);
    CHECK_INT_EQ(static_cast<int>(ext.events.size()), 0);
}

TEST_CASE(RepNotifyFiresOnDeltaForTaggedField) {
    ayt::test::setCurrentCase("RepNotifyFiresOnDeltaForTaggedField");

    ReplicationManager mgr(nullptr);
    RepNotifyRecordingExtension ext;
    mgr.setExtension(&ext);

    RepNotifyObj serverObj;
    serverObj.score = 42;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("RepNotifyObj");
    CHECK(type != nullptr);

    RepNotifyObj clientObj;
    mgr.registerObject(&clientObj, type, 99);

    BitStream body;
    std::vector<uint32_t> dirty{0}; // dense index 0 = "score"
    ReflectSerializer::serializeDirtyFields(type, &serverObj, 99, dirty, body);
    body.resetForRead();

    CHECK(mgr.onReceive(kMsgTypeDelta, body, nullptr));
    CHECK_INT_EQ(clientObj.score, 42);
    CHECK_INT_EQ(static_cast<int>(ext.events.size()), 1);
    if (ext.events.size() >= 1) {
        CHECK(ext.events[0].fieldName == "score");
    }
}

TEST_CASE(NoExtensionSkipsRepNotify) {
    ayt::test::setCurrentCase("NoExtensionSkipsRepNotify");

    ReplicationManager mgr(nullptr);

    RepNotifyObj serverObj;
    serverObj.score = 7;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("RepNotifyObj");
    CHECK(type != nullptr);

    RepNotifyObj clientObj;
    mgr.registerObject(&clientObj, type, 55);

    BitStream body = makeReplicationBody(type, &serverObj, 55);
    CHECK(mgr.onReceive(kMsgTypeReplication, body, nullptr));
    CHECK_INT_EQ(clientObj.score, 7);
}

TEST_SUITE_END
