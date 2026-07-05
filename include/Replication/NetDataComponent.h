#pragma once
// NetDataComponent.h - Macros for marking data components for network replication

#include <IAYNetwork.h>

namespace ayt::net
{

// =============================================================================
// AY_REPLICATABLE - Mark a struct as replicatable over network
// =============================================================================
#define AY_REPLICATABLE(T) \
    static_assert(std::is_trivially_copyable_v<T>, #T " must be trivially copyable"); \
    namespace { \
        struct T##_NetReplicateRegistrar { \
            T##_NetReplicateRegistrar() { \
            } \
        }; \
        static T##_NetReplicateRegistrar T##_g_net_replicate_registrar; \
    }

// =============================================================================
// AY_NET_FIELD - Mark a field for network replication
// =============================================================================
#define AY_NET_FIELD(fieldName) \

} // namespace ayt::net