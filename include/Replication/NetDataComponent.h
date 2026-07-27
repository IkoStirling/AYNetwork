#pragma once
// NetDataComponent.h - Macros for marking data components for network replication
//
// P0 audit fix (2026-07-26):
//   - AY_NET_FIELD was an empty macro (no expansion), causing design §6.2 examples
//     to silently not compile. Deleted. Network replication goes through
//     AYReflect's existing AYTYPE_FIELD_EX(name, field, FieldAttribute::NetReplicate)
//     machinery — see AYNetwork/design.md §6.2 and §13 R3.
//   - AY_REPLICATABLE registrar body was empty; keep the static_assert (it's a
//     useful compile-time guard for trivially copyable constraint) but drop the
//     empty nested struct/registrar so future code can't accidentally depend on it.

#include <IAYNetwork.h>
#include <type_traits>

namespace ayt::net
{

// =============================================================================
// AY_REPLICATABLE - Assert that a struct is safe for network replication
// =============================================================================
// Trivially copyable is required by the bit-level serialization path used in
// ReplicationManager::serializeObject (R1 will switch to AYReflect-driven
// serialization which relaxes this; the assert stays as a safe default).
#define AY_REPLICATABLE(T) \
    static_assert(std::is_trivially_copyable_v<T>, \
        #T " must be trivially copyable for network replication")

} // namespace ayt::net