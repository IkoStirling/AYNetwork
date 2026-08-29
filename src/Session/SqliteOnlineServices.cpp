#include <AYNetwork/Session/SqliteOnlineServices.h>

#include <sodium.h>
#include <sqlite3.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <cstring>
#include <mutex>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

using json = nlohmann::json;
constexpr size_t kMaxNameBytes = 128;
constexpr size_t kMaxKeyBytes = 64;
constexpr size_t kMaxAddressBytes = 255;
constexpr size_t kMaxMetadataEntries = 32;
constexpr size_t kMaxMetadataValueBytes = 256;
constexpr size_t kMaxPasswordBytes = 128;
constexpr size_t kMaxAssignmentBytes = 2u * 1024u * 1024u;
constexpr uint8_t kSchemaVersion = 3;
constexpr char kHex[] = "0123456789abcdef";
constexpr std::array<uint8_t, 32> kStorageCheck = {
    'A','Y','N','e','t','w','o','r','k','-','O','n','l','i','n','e',
    '-','S','t','o','r','a','g','e','-','K','e','y','-','V','1','!'
};

uint64_t systemNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

bool hasNonZero(const std::array<uint8_t, 32>& value) {
    return std::any_of(value.begin(), value.end(),
                       [](uint8_t byte) { return byte != 0; });
}

bool validKey(const std::string& value) {
    return !value.empty() && value.size() <= kMaxKeyBytes;
}

bool validMetadata(const OnlineMetadata& metadata) {
    if (metadata.size() > kMaxMetadataEntries) return false;
    for (const auto& [key, value] : metadata) {
        if (!validKey(key) || value.size() > kMaxMetadataValueBytes) return false;
    }
    return true;
}

std::string metadataToText(const OnlineMetadata& metadata) {
    json value = json::object();
    for (const auto& [key, entry] : metadata) value[key] = entry;
    return value.dump();
}

bool metadataFromText(std::string_view text, OnlineMetadata& metadata) {
    const json value = json::parse(text, nullptr, false);
    if (!value.is_object()) return false;
    OnlineMetadata parsed;
    try {
        for (auto it = value.begin(); it != value.end(); ++it) {
            if (!it.value().is_string()) return false;
            parsed.emplace(it.key(), it.value().get<std::string>());
        }
    } catch (...) { return false; }
    if (!validMetadata(parsed)) return false;
    metadata = std::move(parsed);
    return true;
}

bool metadataContains(const OnlineMetadata& available,
                      const OnlineMetadata& required) {
    for (const auto& [key, value] : required) {
        const auto found = available.find(key);
        if (found == available.end() || found->second != value) return false;
    }
    return true;
}

bool passwordHash(std::string_view password, std::string& encoded) {
    if (password.empty() || password.size() > kMaxPasswordBytes) return false;
    std::array<char, crypto_pwhash_STRBYTES> buffer{};
    if (crypto_pwhash_str_alg(
            buffer.data(), password.data(),
            static_cast<unsigned long long>(password.size()),
            crypto_pwhash_OPSLIMIT_INTERACTIVE,
            crypto_pwhash_MEMLIMIT_INTERACTIVE,
            crypto_pwhash_ALG_ARGON2ID13) != 0) return false;
    encoded.assign(buffer.data());
    sodium_memzero(buffer.data(), buffer.size());
    return true;
}

bool passwordVerify(std::string_view password, const std::string& encoded) {
    return !password.empty() && password.size() <= kMaxPasswordBytes &&
        encoded.size() < crypto_pwhash_STRBYTES &&
        crypto_pwhash_str_verify(
            encoded.c_str(), password.data(),
            static_cast<unsigned long long>(password.size())) == 0;
}

std::array<uint8_t, 32> tokenDigest(std::string_view token) {
    std::array<uint8_t, 32> digest{};
    if (token.size() == 64) {
        (void)crypto_generichash(
            digest.data(), digest.size(),
            reinterpret_cast<const unsigned char*>(token.data()), token.size(),
            nullptr, 0);
    }
    return digest;
}

std::string placementsToText(
    const std::vector<MatchPlayerPlacement>& placements) {
    json value = json::array();
    for (const auto& placement : placements) {
        value.push_back({{"peer", placement.peerId.value},
                         {"team", placement.teamIndex}});
    }
    return value.dump();
}

bool placementsFromText(std::string_view text,
                        std::vector<MatchPlayerPlacement>& placements) {
    const json value = json::parse(text, nullptr, false);
    if (!value.is_array()) return false;
    std::vector<MatchPlayerPlacement> parsed;
    try {
        for (const auto& item : value) {
            MatchPlayerPlacement placement;
            placement.peerId = PeerId{item.at("peer").get<std::string>()};
            placement.teamIndex = item.at("team").get<uint8_t>();
            if (!placement.peerId.isValid()) return false;
            parsed.push_back(std::move(placement));
        }
    } catch (...) { return false; }
    placements = std::move(parsed);
    return true;
}

std::string peersToText(const std::vector<PeerId>& peers) {
    json value = json::array();
    for (const auto& peer : peers) value.push_back(peer.value);
    return value.dump();
}

bool peersFromText(std::string_view text, std::vector<PeerId>& peers) {
    const json value = json::parse(text, nullptr, false);
    if (!value.is_array()) return false;
    std::vector<PeerId> parsed;
    try {
        for (const auto& item : value) {
            PeerId peer{item.get<std::string>()};
            if (!peer.isValid() ||
                std::find(parsed.begin(), parsed.end(), peer) != parsed.end()) {
                return false;
            }
            parsed.push_back(std::move(peer));
        }
    } catch (...) { return false; }
    peers = std::move(parsed);
    return true;
}

bool containsPeer(const std::vector<PeerId>& peers, const PeerId& peer) {
    return std::find(peers.begin(), peers.end(), peer) != peers.end();
}

bool validParty(const std::vector<PeerId>& party, uint16_t target) {
    if (party.empty() || party.size() > target) return false;
    std::unordered_set<std::string> unique;
    for (const PeerId& peer : party) {
        if (!peer.isValid() || !unique.insert(peer.value).second) return false;
    }
    return true;
}

bool matchCompatible(const MatchmakingRequest& left,
                     const MatchmakingRequest& right) {
    const uint32_t skillDistance = left.skillRating > right.skillRating
        ? left.skillRating - right.skillRating
        : right.skillRating - left.skillRating;
    const bool pingCompatible =
        (left.maxPingMs == 0 || right.estimatedPingMs <= left.maxPingMs) &&
        (right.maxPingMs == 0 || left.estimatedPingMs <= right.maxPingMs);
    return left.queue == right.queue && left.region == right.region &&
           left.buildId == right.buildId && left.content == right.content &&
           left.topology == right.topology &&
           left.targetPlayers == right.targetPlayers &&
           left.minimumPlayers == right.minimumPlayers &&
           left.virtualPort == right.virtualPort &&
           left.teamCount == right.teamCount &&
           left.allowBackfill == right.allowBackfill &&
           left.requireAcceptance == right.requireAcceptance &&
           pingCompatible && skillDistance <= left.skillTolerance &&
           skillDistance <= right.skillTolerance;
}

OnlineServiceError mapSessionError(SessionServiceError error) {
    switch (error) {
    case SessionServiceError::InvalidRequest: return OnlineServiceError::InvalidRequest;
    case SessionServiceError::Unauthorized: return OnlineServiceError::Unauthorized;
    case SessionServiceError::SessionFull: return OnlineServiceError::Full;
    case SessionServiceError::SessionClosed: return OnlineServiceError::Closed;
    case SessionServiceError::TransportError:
    case SessionServiceError::RateLimited: return OnlineServiceError::BackendUnavailable;
    case SessionServiceError::None: return OnlineServiceError::None;
    case SessionServiceError::SessionNotFound:
    case SessionServiceError::EpochConflict:
    case SessionServiceError::HostLeaseExpired:
    case SessionServiceError::ProtocolError:
    case SessionServiceError::InternalError: return OnlineServiceError::InternalError;
    }
    return OnlineServiceError::InternalError;
}

std::string randomToken() {
    std::array<uint8_t, 32> bytes{};
    randombytes_buf(bytes.data(), bytes.size());
    std::string token(bytes.size() * 2, '0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        token[i * 2] = kHex[bytes[i] >> 4];
        token[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    sodium_memzero(bytes.data(), bytes.size());
    return token;
}

bool tokenEqual(std::string_view left, std::string_view right) {
    return left.size() == 64 && right.size() == 64 &&
           sodium_memcmp(left.data(), right.data(), left.size()) == 0;
}

class Statement {
public:
    Statement(sqlite3* database, const char* sql) {
        if (database && sqlite3_prepare_v2(database, sql, -1, &_value, nullptr) !=
                            SQLITE_OK) {
            _value = nullptr;
        }
    }
    ~Statement() { if (_value) sqlite3_finalize(_value); }
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    explicit operator bool() const { return _value != nullptr; }
    sqlite3_stmt* get() const { return _value; }

private:
    sqlite3_stmt* _value = nullptr;
};

bool bindU64(sqlite3_stmt* statement, int index, uint64_t value) {
    if (value > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)())) {
        return false;
    }
    return sqlite3_bind_int64(statement, index,
                              static_cast<sqlite3_int64>(value)) == SQLITE_OK;
}

// Session IDs are supplied by an external IP2PSessionService and use the full
// uint64_t domain. SQLite INTEGER is signed, so preserve the two's-complement
// bit pattern instead of rejecting IDs whose high bit is set. Values allocated
// by this store continue to use bindU64() and remain positive SQL integers.
bool bindOpaqueU64(sqlite3_stmt* statement, int index, uint64_t value) {
    static_assert(sizeof(sqlite3_int64) == sizeof(value));
    sqlite3_int64 stored = 0;
    std::memcpy(&stored, &value, sizeof(stored));
    return sqlite3_bind_int64(statement, index, stored) == SQLITE_OK;
}

bool bindText(sqlite3_stmt* statement, int index, std::string_view value) {
    // sqlite3_bind_text treats a null pointer as SQL NULL even when the byte
    // count is zero. A default-constructed empty string_view has a null data()
    // pointer, while several schema fields intentionally persist empty TEXT.
    const char* bytes = value.empty() ? "" : value.data();
    return sqlite3_bind_text(statement, index, bytes,
                             static_cast<int>(value.size()), SQLITE_TRANSIENT) ==
           SQLITE_OK;
}

bool bindBlob(sqlite3_stmt* statement, int index,
              const std::vector<uint8_t>& value) {
    return sqlite3_bind_blob(statement, index, value.data(),
                             static_cast<int>(value.size()), SQLITE_TRANSIENT) ==
           SQLITE_OK;
}

std::string columnText(sqlite3_stmt* statement, int column) {
    const auto* bytes = sqlite3_column_text(statement, column);
    const int size = sqlite3_column_bytes(statement, column);
    if (!bytes || size <= 0) return {};
    return std::string(reinterpret_cast<const char*>(bytes),
                       static_cast<size_t>(size));
}

std::vector<uint8_t> columnBlob(sqlite3_stmt* statement, int column) {
    const auto* bytes = static_cast<const uint8_t*>(
        sqlite3_column_blob(statement, column));
    const int size = sqlite3_column_bytes(statement, column);
    if (!bytes || size <= 0) return {};
    return std::vector<uint8_t>(bytes, bytes + size);
}

bool sealSecret(const std::array<uint8_t, 32>& key, std::string_view context,
                const uint8_t* plain, size_t plainSize,
                std::vector<uint8_t>& sealed) {
    if ((!plain && plainSize != 0) || context.empty()) return false;
    const size_t nonceSize = crypto_aead_xchacha20poly1305_ietf_NPUBBYTES;
    const size_t tagSize = crypto_aead_xchacha20poly1305_ietf_ABYTES;
    if (plainSize > (std::numeric_limits<size_t>::max)() - nonceSize - tagSize) {
        return false;
    }
    sealed.assign(nonceSize + plainSize + tagSize, 0);
    randombytes_buf(sealed.data(), nonceSize);
    unsigned long long cipherSize = 0;
    if (crypto_aead_xchacha20poly1305_ietf_encrypt(
            sealed.data() + nonceSize, &cipherSize, plain, plainSize,
            reinterpret_cast<const uint8_t*>(context.data()), context.size(),
            nullptr, sealed.data(), key.data()) != 0 ||
        cipherSize != plainSize + tagSize) {
        sealed.clear();
        return false;
    }
    return true;
}

bool openSecret(const std::array<uint8_t, 32>& key, std::string_view context,
                const std::vector<uint8_t>& sealed,
                std::vector<uint8_t>& plain) {
    const size_t nonceSize = crypto_aead_xchacha20poly1305_ietf_NPUBBYTES;
    const size_t tagSize = crypto_aead_xchacha20poly1305_ietf_ABYTES;
    plain.clear();
    if (context.empty() || sealed.size() < nonceSize + tagSize) return false;
    plain.assign(sealed.size() - nonceSize - tagSize, 0);
    unsigned long long plainSize = 0;
    if (crypto_aead_xchacha20poly1305_ietf_decrypt(
            plain.data(), &plainSize, nullptr, sealed.data() + nonceSize,
            sealed.size() - nonceSize,
            reinterpret_cast<const uint8_t*>(context.data()), context.size(),
            sealed.data(), key.data()) != 0 || plainSize != plain.size()) {
        if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
        plain.clear();
        return false;
    }
    return true;
}

std::string secretContext(const char* type, uint64_t id) {
    return std::string{"AYNetwork:Online:"} + type + ':' + std::to_string(id) +
           ":v1";
}

json sessionToJson(const P2PBackendSessionInfo& value) {
    return {{"id", value.sessionId}, {"epoch", value.epoch},
            {"host", value.hostPeerId.value}, {"port", value.virtualPort},
            {"capacity", value.capacity}, {"members", value.memberCount},
            {"open", value.open}, {"lease", value.hostLeaseExpiresAtUnixSeconds},
            {"signal_address", value.signalingAddress},
            {"signal_port", value.signalingPort},
            {"signal_room", value.signalingRoom}};
}

bool sessionFromJson(const json& value, P2PBackendSessionInfo& out) {
    try {
        P2PBackendSessionInfo parsed;
        parsed.sessionId = value.at("id").get<uint64_t>();
        parsed.epoch = value.at("epoch").get<uint32_t>();
        parsed.hostPeerId = PeerId{value.at("host").get<std::string>()};
        parsed.virtualPort = value.at("port").get<uint16_t>();
        parsed.capacity = value.at("capacity").get<uint16_t>();
        parsed.memberCount = value.at("members").get<uint16_t>();
        parsed.open = value.at("open").get<bool>();
        parsed.hostLeaseExpiresAtUnixSeconds = value.at("lease").get<uint64_t>();
        parsed.signalingAddress = value.at("signal_address").get<std::string>();
        parsed.signalingPort = value.at("signal_port").get<uint16_t>();
        parsed.signalingRoom = value.at("signal_room").get<std::string>();
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { return false; }
}

json grantToJson(const P2PSessionGrant& value) {
    return {{"session", sessionToJson(value.session)},
            {"peer", value.member.peerId.value},
            {"member_token", value.member.token},
            {"signaling_token", value.signalingToken},
            {"ticket", json::binary(value.joinTicket)},
            {"public_key", json::binary(std::vector<uint8_t>(
                value.ticketPublicKey.begin(), value.ticketPublicKey.end()))}};
}

bool grantFromJson(const json& value, P2PSessionGrant& out) {
    try {
        P2PSessionGrant parsed;
        if (!sessionFromJson(value.at("session"), parsed.session)) return false;
        parsed.member.sessionId = parsed.session.sessionId;
        parsed.member.peerId = PeerId{value.at("peer").get<std::string>()};
        parsed.member.token = value.at("member_token").get<std::string>();
        parsed.signalingToken = value.at("signaling_token").get<std::string>();
        const auto& ticket = value.at("ticket").get_binary();
        const auto& key = value.at("public_key").get_binary();
        parsed.joinTicket.assign(ticket.begin(), ticket.end());
        if (key.size() != parsed.ticketPublicKey.size()) return false;
        std::copy(key.begin(), key.end(), parsed.ticketPublicKey.begin());
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { return false; }
}

json allocationToJson(const DedicatedAllocation& value) {
    return {{"id", value.allocationId}, {"server", value.serverId},
            {"address", value.address}, {"port", value.port},
            {"players", value.playerCount},
            {"token", value.reservationToken},
            {"expires", value.expiresAtUnixSeconds}};
}

bool allocationFromJson(const json& value, DedicatedAllocation& out) {
    try {
        DedicatedAllocation parsed;
        parsed.allocationId = value.at("id").get<uint64_t>();
        parsed.serverId = value.at("server").get<uint64_t>();
        parsed.address = value.at("address").get<std::string>();
        parsed.port = value.at("port").get<uint16_t>();
        parsed.playerCount = value.at("players").get<uint16_t>();
        parsed.reservationToken = value.at("token").get<std::string>();
        parsed.expiresAtUnixSeconds = value.at("expires").get<uint64_t>();
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { return false; }
}

bool serializeAssignment(const MatchAssignment& assignment,
                         std::vector<uint8_t>& bytes) {
    try {
        json grants = json::array();
        for (const auto& grant : assignment.p2pGrants) {
            if (!grant.isValid()) return false;
            grants.push_back(grantToJson(grant));
        }
        json placements = json::array();
        for (const auto& placement : assignment.placements) {
            placements.push_back({{"peer", placement.peerId.value},
                                  {"team", placement.teamIndex}});
        }
        json value{{"match_id", assignment.matchId},
                   {"topology", static_cast<uint8_t>(assignment.topology)},
                   {"content_id", assignment.content.contentId},
                   {"content_version", assignment.content.contentVersion},
                   {"content_seed", assignment.content.contentSeed},
                   {"placements", std::move(placements)},
                   {"grants", std::move(grants)},
                   {"dedicated", assignment.dedicated.isValid()
                        ? allocationToJson(assignment.dedicated) : json(nullptr)}};
        bytes = json::to_cbor(value);
        return !bytes.empty() && bytes.size() <= kMaxAssignmentBytes;
    } catch (...) { bytes.clear(); return false; }
}

bool deserializeAssignment(const std::vector<uint8_t>& bytes,
                           MatchAssignment& out) {
    if (bytes.empty() || bytes.size() > kMaxAssignmentBytes) return false;
    try {
        const json value = json::from_cbor(bytes, true, false);
        if (value.is_discarded()) return false;
        const uint8_t topology = value.at("topology").get<uint8_t>();
        if (topology > static_cast<uint8_t>(MatchTopology::Any)) return false;
        MatchAssignment parsed;
        parsed.matchId = value.at("match_id").get<uint64_t>();
        parsed.topology = static_cast<MatchTopology>(topology);
        parsed.content.contentId = value.at("content_id").get<std::string>();
        parsed.content.contentVersion =
            value.at("content_version").get<std::string>();
        parsed.content.contentSeed = value.at("content_seed").get<uint64_t>();
        if (!parsed.content.isValid()) return false;
        for (const auto& item : value.at("placements")) {
            MatchPlayerPlacement placement;
            placement.peerId = PeerId{item.at("peer").get<std::string>()};
            placement.teamIndex = item.at("team").get<uint8_t>();
            if (!placement.peerId.isValid()) return false;
            parsed.placements.push_back(std::move(placement));
        }
        for (const auto& item : value.at("grants")) {
            P2PSessionGrant grant;
            if (!grantFromJson(item, grant)) return false;
            parsed.p2pGrants.push_back(std::move(grant));
        }
        if (!value.at("dedicated").is_null() &&
            !allocationFromJson(value.at("dedicated"), parsed.dedicated)) {
            return false;
        }
        const bool validP2P = parsed.topology == MatchTopology::P2P &&
                              !parsed.p2pGrants.empty() &&
                              !parsed.dedicated.isValid();
        const bool validDedicated = parsed.topology == MatchTopology::Dedicated &&
                                    parsed.p2pGrants.empty() &&
                                    parsed.dedicated.isValid();
        if (!validP2P && !validDedicated) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { return false; }
}

} // namespace

bool SqliteOnlineServicesConfig::isValid() const {
    return !databasePath.empty() && hasNonZero(storageKey) &&
           busyTimeoutMs != 0 && operationClaimSeconds != 0 &&
           matchTicketRetentionSeconds != 0 &&
           lobbyInvitationMaxLifetimeSeconds != 0 &&
           matchAcceptanceSeconds != 0 &&
           maxLobbies != 0 && maxMatchTickets != 0 &&
           maxDedicatedServers != 0 && maxLobbyCapacity != 0 &&
           maxMatchPlayers >= 2 && maxDedicatedServerCapacity != 0 &&
           dedicatedLeaseSeconds != 0 && allocationLifetimeSeconds != 0;
}

struct SqliteOnlineServices::Impl {
    struct ServerRecord {
        DedicatedServerInfo info;
        std::string token;
    };

    enum class ClaimResult { None, Claimed, Error };

    explicit Impl(SqliteOnlineServicesConfig input,
                  std::shared_ptr<IP2PSessionService> sessions)
        : config(std::move(input)), p2pSessions(std::move(sessions)) {
        if (!config.nowUnixSeconds) config.nowUnixSeconds = systemNow;
        if (!config.isValid() || sodium_init() < 0) {
            lastError = "invalid durable online-services configuration";
            return;
        }
        if (sqlite3_open_v2(config.databasePath.c_str(), &database,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                SQLITE_OPEN_FULLMUTEX,
                            nullptr) != SQLITE_OK) {
            setDatabaseError("failed to open online-services database");
            close();
            return;
        }
        sqlite3_busy_timeout(database, static_cast<int>(config.busyTimeoutMs));
        if (!exec("PRAGMA foreign_keys=ON") ||
            !exec("PRAGMA journal_mode=WAL") ||
            !exec("PRAGMA synchronous=FULL") || !createSchema() ||
            !validateSchemaVersion() || !validateStorageKey() ||
            !recoverExpiredWork()) {
            if (lastError.empty()) {
                setDatabaseError("failed to initialize online-services database");
            }
            close();
            return;
        }
        ready = true;
    }

    ~Impl() { close(); }

    uint64_t now() const { return config.nowUnixSeconds(); }

    void close() {
        ready = false;
        if (database) sqlite3_close(database);
        database = nullptr;
    }

    void setDatabaseError(const char* prefix) {
        lastError = prefix;
        if (database && sqlite3_errcode(database) != SQLITE_OK) {
            lastError += ": ";
            lastError += sqlite3_errmsg(database);
        }
    }

    bool exec(const char* sql) {
        char* error = nullptr;
        const int result = sqlite3_exec(database, sql, nullptr, nullptr, &error);
        if (result == SQLITE_OK) return true;
        lastError = error ? error : "SQLite operation failed";
        sqlite3_free(error);
        return false;
    }

    bool begin() { return exec("BEGIN IMMEDIATE"); }
    bool commit() { return exec("COMMIT"); }
    void rollback() { (void)exec("ROLLBACK"); }

    bool createSchema() {
        return exec(
            "CREATE TABLE IF NOT EXISTS ay_online_meta("
            "name TEXT PRIMARY KEY,value BLOB NOT NULL);"
            "CREATE TABLE IF NOT EXISTS ay_online_lobbies("
            "lobby_id INTEGER PRIMARY KEY,revision INTEGER NOT NULL,"
            "owner_peer TEXT NOT NULL,name TEXT NOT NULL,region TEXT NOT NULL,"
            "build_id TEXT NOT NULL,content_id TEXT NOT NULL,"
            "content_version TEXT NOT NULL,content_seed INTEGER NOT NULL,"
            "capacity INTEGER NOT NULL,state INTEGER NOT NULL,"
            "visibility INTEGER NOT NULL,metadata TEXT NOT NULL,"
            "password_verifier TEXT NOT NULL DEFAULT '',"
            "session_id INTEGER NOT NULL DEFAULT 0,launch_claim TEXT NOT NULL DEFAULT '',"
            "launch_expires INTEGER NOT NULL DEFAULT 0);"
            "CREATE INDEX IF NOT EXISTS ay_online_lobby_filter ON "
            "ay_online_lobbies(state,region,build_id,lobby_id);"
            "CREATE TABLE IF NOT EXISTS ay_online_lobby_members("
            "lobby_id INTEGER NOT NULL,ordinal INTEGER NOT NULL,peer_id TEXT NOT NULL,"
            "PRIMARY KEY(lobby_id,peer_id),UNIQUE(lobby_id,ordinal),"
            "FOREIGN KEY(lobby_id) REFERENCES ay_online_lobbies(lobby_id) ON DELETE CASCADE);"
            "CREATE TABLE IF NOT EXISTS ay_online_lobby_invitations("
            "lobby_id INTEGER NOT NULL,token_hash BLOB NOT NULL,expires INTEGER NOT NULL,"
            "remaining_uses INTEGER NOT NULL,PRIMARY KEY(lobby_id,token_hash),"
            "FOREIGN KEY(lobby_id) REFERENCES ay_online_lobbies(lobby_id) ON DELETE CASCADE);"
            "CREATE TABLE IF NOT EXISTS ay_online_servers("
            "server_id INTEGER PRIMARY KEY,instance_name TEXT NOT NULL UNIQUE,"
            "region TEXT NOT NULL,build_id TEXT NOT NULL,address TEXT NOT NULL,"
            "port INTEGER NOT NULL,capacity INTEGER NOT NULL,reserved_players INTEGER NOT NULL,"
            "draining INTEGER NOT NULL,lease_expires INTEGER NOT NULL,token BLOB NOT NULL);"
            "CREATE INDEX IF NOT EXISTS ay_online_server_filter ON "
            "ay_online_servers(region,build_id,draining,server_id);"
            "CREATE TABLE IF NOT EXISTS ay_online_allocations("
            "allocation_id INTEGER PRIMARY KEY,server_id INTEGER NOT NULL,address TEXT NOT NULL,"
            "port INTEGER NOT NULL,player_count INTEGER NOT NULL,expires INTEGER NOT NULL,"
            "token BLOB NOT NULL,FOREIGN KEY(server_id) REFERENCES "
            "ay_online_servers(server_id) ON DELETE CASCADE);"
            "CREATE INDEX IF NOT EXISTS ay_online_allocation_expiry ON "
            "ay_online_allocations(expires,server_id);"
            "CREATE TABLE IF NOT EXISTS ay_online_tickets("
            "queue_seq INTEGER PRIMARY KEY AUTOINCREMENT,ticket_id INTEGER NOT NULL UNIQUE,"
            "state INTEGER NOT NULL,queue_name TEXT NOT NULL,region TEXT NOT NULL,"
            "build_id TEXT NOT NULL,content_id TEXT NOT NULL,"
            "content_version TEXT NOT NULL,content_seed INTEGER NOT NULL,"
            "topology INTEGER NOT NULL,target_players INTEGER NOT NULL,"
            "minimum_players INTEGER NOT NULL,virtual_port INTEGER NOT NULL,"
            "source_lobby_id INTEGER NOT NULL DEFAULT 0,"
            "source_lobby_revision INTEGER NOT NULL DEFAULT 0,"
            "party_leader TEXT NOT NULL DEFAULT '',"
            "estimated_ping_ms INTEGER NOT NULL DEFAULT 0,"
            "max_ping_ms INTEGER NOT NULL DEFAULT 0,skill_rating INTEGER NOT NULL DEFAULT 0,"
            "skill_tolerance INTEGER NOT NULL DEFAULT 0,team_count INTEGER NOT NULL DEFAULT 2,"
            "allow_backfill INTEGER NOT NULL DEFAULT 0,"
            "require_acceptance INTEGER NOT NULL DEFAULT 0,"
            "failure TEXT NOT NULL DEFAULT '',"
            "assignment BLOB,match_claim TEXT NOT NULL DEFAULT '',"
            "claim_expires INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,"
            "completed_at INTEGER NOT NULL DEFAULT 0,match_id INTEGER NOT NULL DEFAULT 0,"
            "placements TEXT NOT NULL DEFAULT '[]',"
            "accepted_members TEXT NOT NULL DEFAULT '[]',"
            "acceptance_expires INTEGER NOT NULL DEFAULT 0);"
            "CREATE INDEX IF NOT EXISTS ay_online_ticket_queue ON "
            "ay_online_tickets(state,queue_seq);"
            "CREATE TABLE IF NOT EXISTS ay_online_ticket_members("
            "ticket_id INTEGER NOT NULL,ordinal INTEGER NOT NULL,peer_id TEXT NOT NULL,"
            "PRIMARY KEY(ticket_id,peer_id),UNIQUE(ticket_id,ordinal),"
            "FOREIGN KEY(ticket_id) REFERENCES ay_online_tickets(ticket_id) ON DELETE CASCADE);"
            "CREATE INDEX IF NOT EXISTS ay_online_ticket_peer ON "
            "ay_online_ticket_members(peer_id,ticket_id);");
    }

    bool validateSchemaVersion() {
        Statement select(database,
            "SELECT value FROM ay_online_meta WHERE name='schema-version'");
        if (!select) return false;
        const int result = sqlite3_step(select.get());
        if (result == SQLITE_ROW) {
            const auto stored = columnBlob(select.get(), 0);
            const bool valid = stored.size() == 1 &&
                               stored.front() == kSchemaVersion;
            if (!valid) lastError = "unsupported online-services schema version";
            return valid;
        }
        if (result != SQLITE_DONE) return false;
        const std::vector<uint8_t> version{kSchemaVersion};
        Statement insert(database,
            "INSERT INTO ay_online_meta(name,value) VALUES('schema-version',?1)");
        return insert && bindBlob(insert.get(), 1, version) &&
               sqlite3_step(insert.get()) == SQLITE_DONE;
    }

    bool validateStorageKey() {
        Statement select(database,
            "SELECT value FROM ay_online_meta WHERE name='storage-check'");
        if (!select) return false;
        const int result = sqlite3_step(select.get());
        const std::string context = "AYNetwork:Online:storage-check:v1";
        if (result == SQLITE_ROW) {
            std::vector<uint8_t> plain;
            const auto sealed = columnBlob(select.get(), 0);
            const bool valid = openSecret(config.storageKey, context, sealed, plain) &&
                plain.size() == kStorageCheck.size() &&
                sodium_memcmp(plain.data(), kStorageCheck.data(), plain.size()) == 0;
            if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
            if (!valid) lastError = "online-services storage key mismatch";
            return valid;
        }
        if (result != SQLITE_DONE) return false;
        std::vector<uint8_t> sealed;
        if (!sealSecret(config.storageKey, context, kStorageCheck.data(),
                        kStorageCheck.size(), sealed)) return false;
        Statement insert(database,
            "INSERT INTO ay_online_meta(name,value) VALUES('storage-check',?1)");
        return insert && bindBlob(insert.get(), 1, sealed) &&
               sqlite3_step(insert.get()) == SQLITE_DONE;
    }

    bool recoverExpiredWork() {
        const uint64_t current = now();
        Statement lobbies(database,
            "UPDATE ay_online_lobbies SET state=?1,revision=revision+1,"
            "launch_claim='',launch_expires=0 WHERE state=?2 AND launch_expires<=?3");
        Statement tickets(database,
            "UPDATE ay_online_tickets SET state=?1,match_claim='',claim_expires=0 "
            "WHERE state=?2 AND match_claim!='' AND claim_expires<=?3");
        Statement acceptance(database,
            "UPDATE ay_online_tickets SET state=?1,match_id=0,placements='[]',"
            "accepted_members='[]',acceptance_expires=0,"
            "failure='match acceptance timed out; ticket requeued' "
            "WHERE state=?2 AND acceptance_expires<=?3");
        Statement invitations(database,
            "DELETE FROM ay_online_lobby_invitations WHERE expires<=?1 OR "
            "remaining_uses<=0");
        Statement cleanup(database,
            "DELETE FROM ay_online_tickets WHERE completed_at!=0 AND "
            "completed_at<=?1");
        const uint64_t retentionCutoff = current > config.matchTicketRetentionSeconds
            ? current - config.matchTicketRetentionSeconds : 0;
        return lobbies && tickets && acceptance && invitations && cleanup &&
            sqlite3_bind_int(lobbies.get(), 1,
                static_cast<int>(LobbyState::Open)) == SQLITE_OK &&
            sqlite3_bind_int(lobbies.get(), 2,
                static_cast<int>(LobbyState::Launching)) == SQLITE_OK &&
            bindU64(lobbies.get(), 3, current) &&
            sqlite3_step(lobbies.get()) == SQLITE_DONE &&
            sqlite3_bind_int(tickets.get(), 1,
                static_cast<int>(MatchTicketState::Queued)) == SQLITE_OK &&
            sqlite3_bind_int(tickets.get(), 2,
                static_cast<int>(MatchTicketState::Matching)) == SQLITE_OK &&
            bindU64(tickets.get(), 3, current) &&
            sqlite3_step(tickets.get()) == SQLITE_DONE &&
            sqlite3_bind_int(acceptance.get(), 1,
                static_cast<int>(MatchTicketState::Queued)) == SQLITE_OK &&
            sqlite3_bind_int(acceptance.get(), 2,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) == SQLITE_OK &&
            bindU64(acceptance.get(), 3, current) &&
            sqlite3_step(acceptance.get()) == SQLITE_DONE &&
            bindU64(invitations.get(), 1, current) &&
            sqlite3_step(invitations.get()) == SQLITE_DONE &&
            bindU64(cleanup.get(), 1, retentionCutoff) &&
            sqlite3_step(cleanup.get()) == SQLITE_DONE;
    }

    uint64_t allocateId(const char* table, const char* column) {
        const std::string sql = std::string{"SELECT 1 FROM "} + table +
                                " WHERE " + column + "=?1";
        for (unsigned attempt = 0; attempt < 32; ++attempt) {
            uint64_t id = 0;
            randombytes_buf(&id, sizeof(id));
            id &= static_cast<uint64_t>((std::numeric_limits<int64_t>::max)());
            if (id == 0) continue;
            Statement exists(database, sql.c_str());
            if (!exists || !bindU64(exists.get(), 1, id)) return 0;
            if (sqlite3_step(exists.get()) == SQLITE_DONE) return id;
        }
        return 0;
    }

    bool loadLobby(LobbyId lobbyId, LobbyInfo& info) {
        Statement lobby(database,
            "SELECT revision,owner_peer,name,region,build_id,content_id,"
            "content_version,content_seed,capacity,state,visibility,metadata,"
            "password_verifier,session_id "
            "FROM ay_online_lobbies WHERE lobby_id=?1");
        if (!lobby || !bindU64(lobby.get(), 1, lobbyId) ||
            sqlite3_step(lobby.get()) != SQLITE_ROW) return false;
        LobbyInfo parsed;
        parsed.lobbyId = lobbyId;
        parsed.revision = static_cast<uint64_t>(sqlite3_column_int64(lobby.get(), 0));
        parsed.ownerPeerId = PeerId{columnText(lobby.get(), 1)};
        parsed.name = columnText(lobby.get(), 2);
        parsed.region = columnText(lobby.get(), 3);
        parsed.buildId = columnText(lobby.get(), 4);
        parsed.content.contentId = columnText(lobby.get(), 5);
        parsed.content.contentVersion = columnText(lobby.get(), 6);
        parsed.content.contentSeed = static_cast<uint64_t>(
            sqlite3_column_int64(lobby.get(), 7));
        parsed.capacity = static_cast<uint16_t>(sqlite3_column_int(lobby.get(), 8));
        const int state = sqlite3_column_int(lobby.get(), 9);
        const int visibility = sqlite3_column_int(lobby.get(), 10);
        if (state < 0 || state > static_cast<int>(LobbyState::Closed) ||
            visibility < 0 ||
            visibility > static_cast<int>(LobbyVisibility::Private) ||
            !metadataFromText(columnText(lobby.get(), 11), parsed.metadata)) {
            return false;
        }
        parsed.state = static_cast<LobbyState>(state);
        parsed.visibility = static_cast<LobbyVisibility>(visibility);
        parsed.passwordProtected = !columnText(lobby.get(), 12).empty();
        parsed.sessionId = static_cast<uint64_t>(sqlite3_column_int64(lobby.get(), 13));
        Statement members(database,
            "SELECT peer_id FROM ay_online_lobby_members WHERE lobby_id=?1 "
            "ORDER BY ordinal");
        if (!members || !bindU64(members.get(), 1, lobbyId)) return false;
        while (sqlite3_step(members.get()) == SQLITE_ROW) {
            parsed.members.emplace_back(columnText(members.get(), 0));
        }
        if (!parsed.isValid()) return false;
        info = std::move(parsed);
        return true;
    }

    bool loadServer(DedicatedServerId serverId, ServerRecord& record) {
        Statement statement(database,
            "SELECT instance_name,region,build_id,address,port,capacity,"
            "reserved_players,draining,lease_expires,token FROM ay_online_servers "
            "WHERE server_id=?1");
        if (!statement || !bindU64(statement.get(), 1, serverId) ||
            sqlite3_step(statement.get()) != SQLITE_ROW) return false;
        ServerRecord parsed;
        parsed.info.serverId = serverId;
        parsed.info.instanceName = columnText(statement.get(), 0);
        parsed.info.region = columnText(statement.get(), 1);
        parsed.info.buildId = columnText(statement.get(), 2);
        parsed.info.address = columnText(statement.get(), 3);
        parsed.info.port = static_cast<uint16_t>(sqlite3_column_int(statement.get(), 4));
        parsed.info.capacity = static_cast<uint16_t>(sqlite3_column_int(statement.get(), 5));
        parsed.info.reservedPlayers = static_cast<uint16_t>(
            sqlite3_column_int(statement.get(), 6));
        parsed.info.draining = sqlite3_column_int(statement.get(), 7) != 0;
        parsed.info.leaseExpiresAtUnixSeconds = static_cast<uint64_t>(
            sqlite3_column_int64(statement.get(), 8));
        std::vector<uint8_t> plain;
        const auto sealed = columnBlob(statement.get(), 9);
        if (!openSecret(config.storageKey, secretContext("server", serverId),
                        sealed, plain)) return false;
        parsed.token.assign(reinterpret_cast<const char*>(plain.data()), plain.size());
        if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
        if (parsed.info.instanceName.empty() || !validKey(parsed.info.region) ||
            !validKey(parsed.info.buildId) || parsed.info.address.empty() ||
            parsed.info.port == 0 || parsed.info.capacity == 0 ||
            parsed.info.reservedPlayers > parsed.info.capacity ||
            parsed.info.leaseExpiresAtUnixSeconds == 0 || parsed.token.size() != 64) {
            return false;
        }
        record = std::move(parsed);
        return true;
    }

    bool loadTicket(MatchTicketId ticketId, MatchTicketInfo& info) {
        Statement statement(database,
            "SELECT state,queue_name,region,build_id,content_id,content_version,"
            "content_seed,topology,target_players,minimum_players,virtual_port,"
            "source_lobby_id,source_lobby_revision,party_leader,estimated_ping_ms,"
            "max_ping_ms,skill_rating,skill_tolerance,team_count,allow_backfill,"
            "require_acceptance,failure,assignment,match_id,placements,"
            "accepted_members,acceptance_expires "
            "FROM ay_online_tickets WHERE ticket_id=?1");
        if (!statement || !bindU64(statement.get(), 1, ticketId) ||
            sqlite3_step(statement.get()) != SQLITE_ROW) return false;
        MatchTicketInfo parsed;
        parsed.ticketId = ticketId;
        const int state = sqlite3_column_int(statement.get(), 0);
        const int topology = sqlite3_column_int(statement.get(), 7);
        if (state < 0 || state > static_cast<int>(MatchTicketState::Failed) ||
            topology < 0 || topology > static_cast<int>(MatchTopology::Any)) return false;
        parsed.state = static_cast<MatchTicketState>(state);
        parsed.request.queue = columnText(statement.get(), 1);
        parsed.request.region = columnText(statement.get(), 2);
        parsed.request.buildId = columnText(statement.get(), 3);
        parsed.request.content.contentId = columnText(statement.get(), 4);
        parsed.request.content.contentVersion = columnText(statement.get(), 5);
        parsed.request.content.contentSeed = static_cast<uint64_t>(
            sqlite3_column_int64(statement.get(), 6));
        parsed.request.topology = static_cast<MatchTopology>(topology);
        parsed.request.targetPlayers = static_cast<uint16_t>(
            sqlite3_column_int(statement.get(), 8));
        parsed.request.minimumPlayers = static_cast<uint16_t>(
            sqlite3_column_int(statement.get(), 9));
        parsed.request.virtualPort = static_cast<uint16_t>(
            sqlite3_column_int(statement.get(), 10));
        parsed.request.sourceLobbyId = static_cast<uint64_t>(
            sqlite3_column_int64(statement.get(), 11));
        parsed.request.sourceLobbyRevision = static_cast<uint64_t>(
            sqlite3_column_int64(statement.get(), 12));
        parsed.request.partyLeaderPeerId = PeerId{columnText(statement.get(), 13)};
        parsed.request.estimatedPingMs = static_cast<uint16_t>(
            sqlite3_column_int(statement.get(), 14));
        parsed.request.maxPingMs = static_cast<uint16_t>(
            sqlite3_column_int(statement.get(), 15));
        parsed.request.skillRating = static_cast<uint32_t>(
            sqlite3_column_int64(statement.get(), 16));
        parsed.request.skillTolerance = static_cast<uint32_t>(
            sqlite3_column_int64(statement.get(), 17));
        parsed.request.teamCount = static_cast<uint8_t>(
            sqlite3_column_int(statement.get(), 18));
        parsed.request.allowBackfill = sqlite3_column_int(statement.get(), 19) != 0;
        parsed.request.requireAcceptance =
            sqlite3_column_int(statement.get(), 20) != 0;
        parsed.failure = columnText(statement.get(), 21);
        Statement members(database,
            "SELECT peer_id FROM ay_online_ticket_members WHERE ticket_id=?1 "
            "ORDER BY ordinal");
        if (!members || !bindU64(members.get(), 1, ticketId)) return false;
        while (sqlite3_step(members.get()) == SQLITE_ROW) {
            parsed.request.partyMembers.emplace_back(columnText(members.get(), 0));
        }
        if (!validParty(parsed.request.partyMembers, parsed.request.targetPlayers) ||
            !validKey(parsed.request.queue) || !validKey(parsed.request.region) ||
            !validKey(parsed.request.buildId) ||
            !parsed.request.content.isValid() || parsed.request.virtualPort == 0 ||
            parsed.request.minimumPlayers < 2 ||
            parsed.request.minimumPlayers > parsed.request.targetPlayers ||
            parsed.request.teamCount == 0 ||
            parsed.request.teamCount > parsed.request.targetPlayers) {
            return false;
        }
        if (parsed.state == MatchTicketState::Matched) {
            const auto sealed = columnBlob(statement.get(), 22);
            std::vector<uint8_t> plain;
            if (!openSecret(config.storageKey, secretContext("ticket", ticketId),
                            sealed, plain) ||
                !deserializeAssignment(plain, parsed.assignment)) {
                if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
                return false;
            }
            if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
        } else if (parsed.state == MatchTicketState::AwaitingAcceptance ||
                   parsed.state == MatchTicketState::Matching) {
            parsed.assignment.matchId = static_cast<uint64_t>(
                sqlite3_column_int64(statement.get(), 23));
            parsed.assignment.content = parsed.request.content;
            if (!placementsFromText(columnText(statement.get(), 24),
                                    parsed.assignment.placements) ||
                !peersFromText(columnText(statement.get(), 25),
                               parsed.acceptedMembers)) return false;
            parsed.acceptanceExpiresAtUnixSeconds = static_cast<uint64_t>(
                sqlite3_column_int64(statement.get(), 26));
        }
        info = std::move(parsed);
        return true;
    }

    bool isTicketMember(MatchTicketId ticketId, const PeerId& peer) {
        Statement statement(database,
            "SELECT 1 FROM ay_online_ticket_members WHERE ticket_id=?1 AND peer_id=?2");
        return statement && bindU64(statement.get(), 1, ticketId) &&
               bindText(statement.get(), 2, peer.value) &&
               sqlite3_step(statement.get()) == SQLITE_ROW;
    }

    bool expireDedicated() {
        const uint64_t current = now();
        Statement update(database,
            "UPDATE ay_online_servers SET reserved_players=MAX(0,reserved_players-"
            "COALESCE((SELECT SUM(player_count) FROM ay_online_allocations a "
            "WHERE a.server_id=ay_online_servers.server_id AND a.expires<=?1),0)) "
            "WHERE server_id IN (SELECT server_id FROM ay_online_allocations "
            "WHERE expires<=?1)");
        Statement removeAllocations(database,
            "DELETE FROM ay_online_allocations WHERE expires<=?1");
        Statement removeServers(database,
            "DELETE FROM ay_online_servers WHERE lease_expires<=?1");
        return update && removeAllocations && removeServers &&
               bindU64(update.get(), 1, current) &&
               sqlite3_step(update.get()) == SQLITE_DONE &&
               bindU64(removeAllocations.get(), 1, current) &&
               sqlite3_step(removeAllocations.get()) == SQLITE_DONE &&
               bindU64(removeServers.get(), 1, current) &&
               sqlite3_step(removeServers.get()) == SQLITE_DONE;
    }

    int rowExists(const char* table, const char* column, uint64_t id) {
        const std::string sql = std::string{"SELECT 1 FROM "} + table +
                                " WHERE " + column + "=?1";
        Statement statement(database, sql.c_str());
        if (!statement || !bindU64(statement.get(), 1, id)) return -1;
        const int result = sqlite3_step(statement.get());
        if (result == SQLITE_ROW) return 1;
        return result == SQLITE_DONE ? 0 : -1;
    }

    ClaimResult claimMatchBatch(std::vector<MatchTicketId>& batch,
                                MatchmakingRequest& request,
                                std::vector<PeerId>& peers,
                                std::string& claim,
                                uint64_t& matchId,
                                std::vector<MatchPlayerPlacement>& placements) {
        batch.clear();
        peers.clear();
        claim.clear();
        matchId = 0;
        placements.clear();
        if (!recoverExpiredWork() || !begin()) return ClaimResult::Error;
        Statement ready(database,
            "SELECT match_id FROM ay_online_tickets WHERE state=?1 AND "
            "match_id!=0 AND match_claim='' ORDER BY queue_seq LIMIT 1");
        if (!ready || sqlite3_bind_int(ready.get(), 1,
                static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK) {
            rollback();
            return ClaimResult::Error;
        }
        const int readyResult = sqlite3_step(ready.get());
        if (readyResult == SQLITE_ROW) {
            matchId = static_cast<uint64_t>(sqlite3_column_int64(ready.get(), 0));
            Statement members(database,
                "SELECT ticket_id FROM ay_online_tickets WHERE state=?1 AND "
                "match_id=?2 AND match_claim='' ORDER BY queue_seq");
            if (!members || sqlite3_bind_int(members.get(), 1,
                    static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
                !bindU64(members.get(), 2, matchId)) {
                rollback();
                return ClaimResult::Error;
            }
            int result = SQLITE_ROW;
            while ((result = sqlite3_step(members.get())) == SQLITE_ROW) {
                const MatchTicketId id = static_cast<uint64_t>(
                    sqlite3_column_int64(members.get(), 0));
                MatchTicketInfo ticket;
                if (!loadTicket(id, ticket)) {
                    rollback();
                    return ClaimResult::Error;
                }
                if (batch.empty()) {
                    request = ticket.request;
                    placements = ticket.assignment.placements;
                }
                batch.push_back(id);
                peers.insert(peers.end(), ticket.request.partyMembers.begin(),
                             ticket.request.partyMembers.end());
            }
            if (result != SQLITE_DONE || batch.empty()) {
                rollback();
                return ClaimResult::Error;
            }
            claim = randomToken();
            const uint64_t expiry = now() + config.operationClaimSeconds;
            for (MatchTicketId id : batch) {
                Statement update(database,
                    "UPDATE ay_online_tickets SET match_claim=?1,claim_expires=?2 "
                    "WHERE ticket_id=?3 AND state=?4 AND match_id=?5 AND "
                    "match_claim=''");
                if (claim.empty() || !update || !bindText(update.get(), 1, claim) ||
                    !bindU64(update.get(), 2, expiry) ||
                    !bindU64(update.get(), 3, id) ||
                    sqlite3_bind_int(update.get(), 4,
                        static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
                    !bindU64(update.get(), 5, matchId) ||
                    sqlite3_step(update.get()) != SQLITE_DONE ||
                    sqlite3_changes(database) != 1) {
                    rollback();
                    return ClaimResult::Error;
                }
            }
            if (!commit()) {
                rollback();
                return ClaimResult::Error;
            }
            return ClaimResult::Claimed;
        }
        if (readyResult != SQLITE_DONE) {
            rollback();
            return ClaimResult::Error;
        }
        Statement queued(database,
            "SELECT ticket_id FROM ay_online_tickets WHERE state=?1 "
            "ORDER BY queue_seq");
        if (!queued || sqlite3_bind_int(queued.get(), 1,
                static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK) {
            rollback();
            return ClaimResult::Error;
        }
        std::vector<std::pair<MatchTicketId, MatchTicketInfo>> candidates;
        int step = SQLITE_ROW;
        while ((step = sqlite3_step(queued.get())) == SQLITE_ROW) {
            const MatchTicketId id = static_cast<uint64_t>(
                sqlite3_column_int64(queued.get(), 0));
            MatchTicketInfo info;
            if (!loadTicket(id, info)) {
                rollback();
                return ClaimResult::Error;
            }
            candidates.emplace_back(id, std::move(info));
        }
        if (step != SQLITE_DONE) {
            rollback();
            return ClaimResult::Error;
        }
        for (size_t seedIndex = 0; seedIndex < candidates.size(); ++seedIndex) {
            const auto& seed = candidates[seedIndex];
            size_t players = seed.second.request.partyMembers.size();
            std::vector<MatchTicketId> selected{seed.first};
            for (size_t candidateIndex = 0;
                 candidateIndex < candidates.size(); ++candidateIndex) {
                if (candidateIndex == seedIndex) continue;
                const auto& candidate = candidates[candidateIndex];
                if (!matchCompatible(seed.second.request,
                                     candidate.second.request)) continue;
                const size_t party = candidate.second.request.partyMembers.size();
                if (players + party > seed.second.request.targetPlayers) continue;
                selected.push_back(candidate.first);
                players += party;
                if (players == seed.second.request.targetPlayers) break;
            }
            const size_t required = seed.second.request.allowBackfill
                ? seed.second.request.minimumPlayers
                : seed.second.request.targetPlayers;
            if (players >= required) {
                batch = std::move(selected);
                request = seed.second.request;
                break;
            }
        }
        if (batch.empty()) {
            if (!commit()) {
                rollback();
                return ClaimResult::Error;
            }
            return ClaimResult::None;
        }
        claim = randomToken();
        const uint64_t expiry = now() + config.operationClaimSeconds;
        for (MatchTicketId id : batch) {
            Statement update(database,
                "UPDATE ay_online_tickets SET state=?1,match_claim=?2,"
                "claim_expires=?3 WHERE ticket_id=?4 AND state=?5");
            if (claim.empty() || !update ||
                sqlite3_bind_int(update.get(), 1,
                    static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
                !bindText(update.get(), 2, claim) || !bindU64(update.get(), 3, expiry) ||
                !bindU64(update.get(), 4, id) ||
                sqlite3_bind_int(update.get(), 5,
                    static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK ||
                sqlite3_step(update.get()) != SQLITE_DONE ||
                sqlite3_changes(database) != 1) {
                rollback();
                batch.clear();
                return ClaimResult::Error;
            }
        }
        for (MatchTicketId id : batch) {
            const auto candidate = std::find_if(
                candidates.begin(), candidates.end(), [id](const auto& value) {
                    return value.first == id;
                });
            if (candidate == candidates.end()) {
                rollback();
                batch.clear();
                return ClaimResult::Error;
            }
            peers.insert(peers.end(), candidate->second.request.partyMembers.begin(),
                         candidate->second.request.partyMembers.end());
        }
        std::vector<size_t> teamSizes(request.teamCount, 0);
        for (MatchTicketId id : batch) {
            const auto candidate = std::find_if(
                candidates.begin(), candidates.end(), [id](const auto& value) {
                    return value.first == id;
                });
            if (candidate == candidates.end()) {
                rollback();
                return ClaimResult::Error;
            }
            const auto smallest = std::min_element(
                teamSizes.begin(), teamSizes.end());
            const uint8_t team = static_cast<uint8_t>(
                std::distance(teamSizes.begin(), smallest));
            *smallest += candidate->second.request.partyMembers.size();
            for (const PeerId& peer : candidate->second.request.partyMembers) {
                placements.push_back({peer, team});
            }
        }
        if (!commit()) {
            rollback();
            batch.clear();
            return ClaimResult::Error;
        }
        return ClaimResult::Claimed;
    }

    bool stageMatchAcceptance(
        const std::vector<MatchTicketId>& batch, const std::string& claim,
        uint64_t matchId,
        const std::vector<MatchPlayerPlacement>& placements) {
        if (batch.empty() || claim.empty() || matchId == 0 ||
            placements.empty() || !begin()) return false;
        const std::string placementText = placementsToText(placements);
        const uint64_t expires = now() + config.matchAcceptanceSeconds;
        for (MatchTicketId id : batch) {
            Statement update(database,
                "UPDATE ay_online_tickets SET state=?1,match_id=?2,placements=?3,"
                "accepted_members='[]',acceptance_expires=?4,match_claim='',"
                "claim_expires=0,failure='' WHERE ticket_id=?5 AND state=?6 "
                "AND match_claim=?7");
            if (!update || sqlite3_bind_int(update.get(), 1,
                    static_cast<int>(MatchTicketState::AwaitingAcceptance)) !=
                    SQLITE_OK || !bindU64(update.get(), 2, matchId) ||
                !bindText(update.get(), 3, placementText) ||
                !bindU64(update.get(), 4, expires) ||
                !bindU64(update.get(), 5, id) ||
                sqlite3_bind_int(update.get(), 6,
                    static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
                !bindText(update.get(), 7, claim) ||
                sqlite3_step(update.get()) != SQLITE_DONE ||
                sqlite3_changes(database) != 1) {
                rollback();
                return false;
            }
        }
        if (!commit()) {
            rollback();
            return false;
        }
        return true;
    }

    bool resetMatchBatch(const std::vector<MatchTicketId>& batch,
                         const std::string& claim) {
        if (!begin()) return false;
        for (MatchTicketId id : batch) {
            Statement update(database,
                "UPDATE ay_online_tickets SET state=?1,match_claim='',"
                "claim_expires=0,match_id=0,placements='[]',accepted_members='[]',"
                "acceptance_expires=0 WHERE ticket_id=?2 AND state=?3 "
                "AND match_claim=?4");
            if (!update || sqlite3_bind_int(update.get(), 1,
                    static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK ||
                !bindU64(update.get(), 2, id) ||
                sqlite3_bind_int(update.get(), 3,
                    static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
                !bindText(update.get(), 4, claim) ||
                sqlite3_step(update.get()) != SQLITE_DONE ||
                sqlite3_changes(database) != 1) {
                rollback();
                return false;
            }
        }
        if (!commit()) {
            rollback();
            return false;
        }
        return true;
    }

    bool finalizeMatchBatch(const std::vector<MatchTicketId>& batch,
                            const std::string& claim,
                            const MatchAssignment& assignment,
                            OnlineServiceError failure,
                            const std::string& failureMessage) {
        if (!begin()) return false;
        for (MatchTicketId id : batch) {
            MatchTicketInfo ticket;
            if (!loadTicket(id, ticket)) {
                rollback();
                return false;
            }
            std::vector<uint8_t> sealed;
            if (failure == OnlineServiceError::None) {
                MatchAssignment perTicket;
                perTicket.matchId = assignment.matchId;
                perTicket.topology = assignment.topology;
                perTicket.content = assignment.content;
                perTicket.placements = assignment.placements;
                if (assignment.topology == MatchTopology::Dedicated) {
                    perTicket.dedicated = assignment.dedicated;
                } else {
                    for (const auto& grant : assignment.p2pGrants) {
                        if (containsPeer(ticket.request.partyMembers,
                                         grant.member.peerId)) {
                            perTicket.p2pGrants.push_back(grant);
                        }
                    }
                }
                std::vector<uint8_t> plain;
                if (!serializeAssignment(perTicket, plain) ||
                    !sealSecret(config.storageKey, secretContext("ticket", id),
                                plain.data(), plain.size(), sealed)) {
                    if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
                    rollback();
                    return false;
                }
                if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
            }
            Statement update(database,
                "UPDATE ay_online_tickets SET state=?1,failure=?2,assignment=?3,"
                "match_claim='',claim_expires=0,match_id=0,placements='[]',"
                "accepted_members='[]',acceptance_expires=0,completed_at=?4 "
                "WHERE ticket_id=?5 "
                "AND state=?6 AND match_claim=?7");
            if (!update) {
                rollback();
                return false;
            }
            const std::string detail = failureMessage.size() <= 512
                ? failureMessage : failureMessage.substr(0, 512);
            const bool boundAssignment = failure == OnlineServiceError::None
                ? bindBlob(update.get(), 3, sealed)
                : sqlite3_bind_null(update.get(), 3) == SQLITE_OK;
            if (sqlite3_bind_int(update.get(), 1,
                    failure == OnlineServiceError::None
                        ? static_cast<int>(MatchTicketState::Matched)
                        : static_cast<int>(MatchTicketState::Failed)) != SQLITE_OK ||
                !bindText(update.get(), 2, failure == OnlineServiceError::None
                    ? std::string_view{} : std::string_view{detail}) ||
                !boundAssignment || !bindU64(update.get(), 4, now()) ||
                !bindU64(update.get(), 5, id) ||
                sqlite3_bind_int(update.get(), 6,
                    static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
                !bindText(update.get(), 7, claim) ||
                sqlite3_step(update.get()) != SQLITE_DONE ||
                sqlite3_changes(database) != 1) {
                rollback();
                return false;
            }
        }
        if (!commit()) {
            rollback();
            return false;
        }
        return true;
    }

    template <typename T>
    OnlineServiceResult<T> databaseFailure(const char* message) {
        setDatabaseError(message);
        return OnlineServiceResult<T>::failure(
            OnlineServiceError::BackendUnavailable, lastError);
    }

    SqliteOnlineServicesConfig config;
    std::shared_ptr<IP2PSessionService> p2pSessions;
    sqlite3* database = nullptr;
    mutable std::recursive_mutex mutex;
    bool ready = false;
    std::string lastError;
};

SqliteOnlineServices::SqliteOnlineServices(
    SqliteOnlineServicesConfig config,
    std::shared_ptr<IP2PSessionService> p2pSessions)
    : _impl(std::make_unique<Impl>(std::move(config), std::move(p2pSessions))) {}

SqliteOnlineServices::~SqliteOnlineServices() = default;

bool SqliteOnlineServices::isReady() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->ready;
}

std::string SqliteOnlineServices::getLastError() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->lastError;
}

OnlineServiceResult<LobbyInfo> SqliteOnlineServices::createLobby(
    const CreateLobbyRequest& request) {
    if (!request.ownerPeerId.isValid() || request.name.empty() ||
        request.name.size() > kMaxNameBytes || !validKey(request.region) ||
        !validKey(request.buildId) || !request.content.isValid() ||
        !validMetadata(request.metadata) ||
        request.password.size() > kMaxPasswordBytes || request.capacity == 0 ||
        request.capacity > _impl->config.maxLobbyCapacity ||
        static_cast<uint8_t>(request.visibility) >
            static_cast<uint8_t>(LobbyVisibility::Private)) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid durable lobby request");
    }
    std::string passwordVerifier;
    if (!request.password.empty() &&
        !passwordHash(request.password, passwordVerifier)) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InternalError,
            "failed to protect durable lobby password");
    }
    const std::string metadata = metadataToText(request.metadata);
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin()) {
        return _impl->databaseFailure<LobbyInfo>("failed to begin lobby create");
    }
    Statement count(_impl->database, "SELECT COUNT(*) FROM ay_online_lobbies");
    if (!count || sqlite3_step(count.get()) != SQLITE_ROW) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInfo>("failed to count lobbies");
    }
    if (static_cast<uint64_t>(sqlite3_column_int64(count.get(), 0)) >=
        _impl->config.maxLobbies) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Full, "durable lobby directory is full");
    }
    const LobbyId id = _impl->allocateId(
        "ay_online_lobbies", "lobby_id");
    Statement insertLobby(_impl->database,
        "INSERT INTO ay_online_lobbies(lobby_id,revision,owner_peer,name,region,"
        "build_id,content_id,content_version,content_seed,capacity,state,"
        "visibility,metadata,password_verifier,session_id) "
        "VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0)");
    Statement insertMember(_impl->database,
        "INSERT INTO ay_online_lobby_members(lobby_id,ordinal,peer_id) "
        "VALUES(?1,0,?2)");
    const bool inserted = id != 0 && insertLobby && insertMember &&
        bindU64(insertLobby.get(), 1, id) &&
        bindText(insertLobby.get(), 2, request.ownerPeerId.value) &&
        bindText(insertLobby.get(), 3, request.name) &&
        bindText(insertLobby.get(), 4, request.region) &&
        bindText(insertLobby.get(), 5, request.buildId) &&
        bindText(insertLobby.get(), 6, request.content.contentId) &&
        bindText(insertLobby.get(), 7, request.content.contentVersion) &&
        bindU64(insertLobby.get(), 8, request.content.contentSeed) &&
        sqlite3_bind_int(insertLobby.get(), 9, request.capacity) == SQLITE_OK &&
        sqlite3_bind_int(insertLobby.get(), 10,
            static_cast<int>(LobbyState::Open)) == SQLITE_OK &&
        sqlite3_bind_int(insertLobby.get(), 11,
            static_cast<int>(request.visibility)) == SQLITE_OK &&
        bindText(insertLobby.get(), 12, metadata) &&
        bindText(insertLobby.get(), 13, passwordVerifier) &&
        sqlite3_step(insertLobby.get()) == SQLITE_DONE &&
        bindU64(insertMember.get(), 1, id) &&
        bindText(insertMember.get(), 2, request.ownerPeerId.value) &&
        sqlite3_step(insertMember.get()) == SQLITE_DONE;
    LobbyInfo info;
    if (!inserted || !_impl->loadLobby(id, info) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInfo>("failed to persist lobby");
    }
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<std::vector<LobbyInfo>>
SqliteOnlineServices::listLobbies(const ListLobbiesRequest& request) {
    if (request.limit == 0 || request.limit > 1000 ||
        request.region.size() > kMaxKeyBytes ||
        request.buildId.size() > kMaxKeyBytes ||
        request.contentId.size() > 128 || !validMetadata(request.metadata)) {
        return OnlineServiceResult<std::vector<LobbyInfo>>::failure(
            OnlineServiceError::InvalidRequest, "invalid durable lobby filter");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<std::vector<LobbyInfo>>(
            "failed to query durable lobbies");
    }
    Statement statement(_impl->database,
        "SELECT lobby_id FROM ay_online_lobbies l WHERE state=?1 "
        "AND visibility=?2 AND (?3='' OR region=?3) "
        "AND (?4='' OR build_id=?4) AND (?5='' OR content_id=?5) "
        "AND capacity-(SELECT COUNT(*) FROM ay_online_lobby_members m "
        "WHERE m.lobby_id=l.lobby_id)>=?6 ORDER BY lobby_id");
    if (!statement ||
        sqlite3_bind_int(statement.get(), 1,
            static_cast<int>(LobbyState::Open)) != SQLITE_OK ||
        sqlite3_bind_int(statement.get(), 2,
            static_cast<int>(LobbyVisibility::Public)) != SQLITE_OK ||
        !bindText(statement.get(), 3, request.region) ||
        !bindText(statement.get(), 4, request.buildId) ||
        !bindText(statement.get(), 5, request.contentId) ||
        sqlite3_bind_int64(statement.get(), 6,
            static_cast<sqlite3_int64>(request.minimumOpenSlots)) != SQLITE_OK) {
        _impl->rollback();
        return _impl->databaseFailure<std::vector<LobbyInfo>>(
            "failed to prepare lobby query");
    }
    std::vector<LobbyInfo> result;
    int step = SQLITE_ROW;
    while ((step = sqlite3_step(statement.get())) == SQLITE_ROW) {
        LobbyInfo info;
        const LobbyId id = static_cast<LobbyId>(
            sqlite3_column_int64(statement.get(), 0));
        if (!_impl->loadLobby(id, info)) {
            _impl->rollback();
            return _impl->databaseFailure<std::vector<LobbyInfo>>(
                "invalid durable lobby record");
        }
        if (!metadataContains(info.metadata, request.metadata)) continue;
        result.push_back(std::move(info));
        if (result.size() == request.limit) break;
    }
    if (step != SQLITE_DONE || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<std::vector<LobbyInfo>>(
            "failed to read durable lobbies");
    }
    return OnlineServiceResult<std::vector<LobbyInfo>>::success(std::move(result));
}

OnlineServiceResult<LobbyInfo> SqliteOnlineServices::joinLobby(
    LobbyId lobbyId, const PeerId& authenticatedPeer) {
    return joinLobby(JoinLobbyRequest{lobbyId, authenticatedPeer});
}

OnlineServiceResult<LobbyInfo> SqliteOnlineServices::joinLobby(
    const JoinLobbyRequest& request) {
    if (request.lobbyId == 0 || !request.authenticatedPeer.isValid() ||
        request.password.size() > kMaxPasswordBytes ||
        (!request.invitationToken.empty() &&
         request.invitationToken.size() != 64)) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid durable lobby join");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<LobbyInfo>("failed to begin lobby join");
    }
    LobbyInfo info;
    if (!_impl->loadLobby(request.lobbyId, info)) {
        const int exists = _impl->rowExists(
            "ay_online_lobbies", "lobby_id", request.lobbyId);
        _impl->rollback();
        if (exists == 0) return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
        return _impl->databaseFailure<LobbyInfo>("failed to load lobby");
    }
    if (containsPeer(info.members, request.authenticatedPeer)) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<LobbyInfo>("failed to finish lobby join");
        }
        return OnlineServiceResult<LobbyInfo>::success(std::move(info));
    }
    if (info.state != LobbyState::Open) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Closed, "lobby is not joinable");
    }
    if (info.members.size() >= info.capacity) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Full, "lobby is full");
    }
    bool invited = false;
    std::array<uint8_t, 32> invitationHash{};
    if (!request.invitationToken.empty()) {
        invitationHash = tokenDigest(request.invitationToken);
        Statement invitation(_impl->database,
            "SELECT expires,remaining_uses FROM ay_online_lobby_invitations "
            "WHERE lobby_id=?1 AND token_hash=?2");
        if (!invitation || !bindU64(invitation.get(), 1, request.lobbyId) ||
            sqlite3_bind_blob(invitation.get(), 2, invitationHash.data(),
                static_cast<int>(invitationHash.size()), SQLITE_TRANSIENT) !=
                SQLITE_OK) {
            _impl->rollback();
            return _impl->databaseFailure<LobbyInfo>(
                "failed to query lobby invitation");
        }
        if (sqlite3_step(invitation.get()) == SQLITE_ROW) {
            const uint64_t expires = static_cast<uint64_t>(
                sqlite3_column_int64(invitation.get(), 0));
            invited = _impl->now() < expires &&
                sqlite3_column_int(invitation.get(), 1) > 0;
        }
    }
    bool passwordAccepted = !info.passwordProtected;
    if (info.passwordProtected && !request.password.empty()) {
        Statement verifier(_impl->database,
            "SELECT password_verifier FROM ay_online_lobbies WHERE lobby_id=?1");
        if (!verifier || !bindU64(verifier.get(), 1, request.lobbyId) ||
            sqlite3_step(verifier.get()) != SQLITE_ROW) {
            _impl->rollback();
            return _impl->databaseFailure<LobbyInfo>(
                "failed to read lobby password verifier");
        }
        passwordAccepted = passwordVerify(
            request.password, columnText(verifier.get(), 0));
    }
    if ((info.visibility == LobbyVisibility::Private && !invited) ||
        (!invited && !passwordAccepted)) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Unauthorized,
            "lobby credentials were rejected");
    }
    if (invited) {
        Statement consume(_impl->database,
            "UPDATE ay_online_lobby_invitations SET "
            "remaining_uses=remaining_uses-1 WHERE lobby_id=?1 AND token_hash=?2 "
            "AND remaining_uses>0");
        if (!consume || !bindU64(consume.get(), 1, request.lobbyId) ||
            sqlite3_bind_blob(consume.get(), 2, invitationHash.data(),
                static_cast<int>(invitationHash.size()), SQLITE_TRANSIENT) !=
                SQLITE_OK || sqlite3_step(consume.get()) != SQLITE_DONE ||
            sqlite3_changes(_impl->database) != 1) {
            _impl->rollback();
            return _impl->databaseFailure<LobbyInfo>(
                "failed to consume lobby invitation");
        }
    }
    Statement insert(_impl->database,
        "INSERT INTO ay_online_lobby_members(lobby_id,ordinal,peer_id) "
        "VALUES(?1,(SELECT COALESCE(MAX(ordinal),-1)+1 FROM "
        "ay_online_lobby_members WHERE lobby_id=?1),?2)");
    Statement update(_impl->database,
        "UPDATE ay_online_lobbies SET revision=revision+1 WHERE lobby_id=?1");
    if (!insert || !update || !bindU64(insert.get(), 1, request.lobbyId) ||
        !bindText(insert.get(), 2, request.authenticatedPeer.value) ||
        sqlite3_step(insert.get()) != SQLITE_DONE ||
        !bindU64(update.get(), 1, request.lobbyId) ||
        sqlite3_step(update.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 ||
        !_impl->loadLobby(request.lobbyId, info) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInfo>("failed to persist lobby join");
    }
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInfo> SqliteOnlineServices::leaveLobby(
    LobbyId lobbyId, const PeerId& authenticatedPeer) {
    if (lobbyId == 0 || !authenticatedPeer.isValid()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid durable lobby leave");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<LobbyInfo>("failed to begin lobby leave");
    }
    LobbyInfo info;
    if (!_impl->loadLobby(lobbyId, info)) {
        const int exists = _impl->rowExists(
            "ay_online_lobbies", "lobby_id", lobbyId);
        _impl->rollback();
        if (exists == 0) return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
        return _impl->databaseFailure<LobbyInfo>("failed to load lobby");
    }
    if (info.state == LobbyState::Launching) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Conflict, "lobby launch is in progress");
    }
    const auto member = std::find(
        info.members.begin(), info.members.end(), authenticatedPeer);
    if (member == info.members.end()) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Unauthorized, "peer is not a lobby member");
    }
    if (info.members.size() == 1) {
        LobbyInfo closed = info;
        closed.state = LobbyState::Closed;
        closed.sessionId = 0;
        ++closed.revision;
        Statement remove(_impl->database,
            "DELETE FROM ay_online_lobbies WHERE lobby_id=?1");
        if (!remove || !bindU64(remove.get(), 1, lobbyId) ||
            sqlite3_step(remove.get()) != SQLITE_DONE ||
            sqlite3_changes(_impl->database) != 1 || !_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<LobbyInfo>("failed to remove lobby");
        }
        return OnlineServiceResult<LobbyInfo>::success(std::move(closed));
    }
    Statement remove(_impl->database,
        "DELETE FROM ay_online_lobby_members WHERE lobby_id=?1 AND peer_id=?2");
    if (!remove || !bindU64(remove.get(), 1, lobbyId) ||
        !bindText(remove.get(), 2, authenticatedPeer.value) ||
        sqlite3_step(remove.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInfo>("failed to remove lobby member");
    }
    std::string newOwner = info.ownerPeerId.value;
    if (info.ownerPeerId == authenticatedPeer) {
        Statement owner(_impl->database,
            "SELECT peer_id FROM ay_online_lobby_members WHERE lobby_id=?1 "
            "ORDER BY ordinal LIMIT 1");
        if (!owner || !bindU64(owner.get(), 1, lobbyId) ||
            sqlite3_step(owner.get()) != SQLITE_ROW) {
            _impl->rollback();
            return _impl->databaseFailure<LobbyInfo>("failed to transfer lobby owner");
        }
        newOwner = columnText(owner.get(), 0);
    }
    Statement update(_impl->database,
        "UPDATE ay_online_lobbies SET owner_peer=?1,revision=revision+1 "
        "WHERE lobby_id=?2");
    if (!update || !bindText(update.get(), 1, newOwner) ||
        !bindU64(update.get(), 2, lobbyId) ||
        sqlite3_step(update.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 ||
        !_impl->loadLobby(lobbyId, info) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInfo>("failed to persist lobby leave");
    }
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInfo> SqliteOnlineServices::updateLobby(
    const UpdateLobbyRequest& request) {
    if (request.lobbyId == 0 || !request.actorPeerId.isValid() ||
        request.expectedRevision == 0 || request.name.empty() ||
        request.name.size() > kMaxNameBytes ||
        (request.replaceMetadata && !validMetadata(request.metadata)) ||
        request.password.size() > kMaxPasswordBytes ||
        (request.setVisibility &&
         static_cast<uint8_t>(request.visibility) >
             static_cast<uint8_t>(LobbyVisibility::Private))) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid durable lobby update");
    }
    const std::string metadata = metadataToText(request.metadata);
    std::string passwordVerifier;
    if (request.setPassword && !request.password.empty() &&
        !passwordHash(request.password, passwordVerifier)) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InternalError,
            "failed to protect durable lobby password");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<LobbyInfo>("failed to begin lobby update");
    }
    LobbyInfo info;
    if (!_impl->loadLobby(request.lobbyId, info)) {
        const int exists = _impl->rowExists(
            "ay_online_lobbies", "lobby_id", request.lobbyId);
        _impl->rollback();
        if (exists == 0) return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
        return _impl->databaseFailure<LobbyInfo>("failed to load lobby");
    }
    if (info.ownerPeerId != request.actorPeerId) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Unauthorized, "only the lobby owner may update it");
    }
    if (info.revision != request.expectedRevision) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Conflict, "lobby revision changed");
    }
    if (info.state != LobbyState::Open) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Closed, "lobby is not open");
    }
    Statement update(_impl->database,
        "UPDATE ay_online_lobbies SET name=?1,"
        "metadata=CASE WHEN ?2 THEN ?3 ELSE metadata END,"
        "visibility=CASE WHEN ?4 THEN ?5 ELSE visibility END,"
        "password_verifier=CASE WHEN ?6 THEN ?7 ELSE password_verifier END,"
        "revision=revision+1 WHERE lobby_id=?8 AND revision=?9 AND state=?10");
    if (!update || !bindText(update.get(), 1, request.name) ||
        sqlite3_bind_int(update.get(), 2, request.replaceMetadata ? 1 : 0) !=
            SQLITE_OK || !bindText(update.get(), 3, metadata) ||
        sqlite3_bind_int(update.get(), 4, request.setVisibility ? 1 : 0) !=
            SQLITE_OK || sqlite3_bind_int(update.get(), 5,
                static_cast<int>(request.visibility)) != SQLITE_OK ||
        sqlite3_bind_int(update.get(), 6, request.setPassword ? 1 : 0) !=
            SQLITE_OK || !bindText(update.get(), 7, passwordVerifier) ||
        !bindU64(update.get(), 8, request.lobbyId) ||
        !bindU64(update.get(), 9, request.expectedRevision) ||
        sqlite3_bind_int(update.get(), 10,
            static_cast<int>(LobbyState::Open)) != SQLITE_OK ||
        sqlite3_step(update.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 ||
        !_impl->loadLobby(request.lobbyId, info) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInfo>("failed to persist lobby update");
    }
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInvitation>
SqliteOnlineServices::createLobbyInvitation(
    const CreateLobbyInvitationRequest& request) {
    if (request.lobbyId == 0 || !request.actorPeerId.isValid() ||
        request.expectedRevision == 0 || request.lifetimeSeconds == 0 ||
        request.lifetimeSeconds >
            _impl->config.lobbyInvitationMaxLifetimeSeconds ||
        request.maxUses == 0) {
        return OnlineServiceResult<LobbyInvitation>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable lobby invitation");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<LobbyInvitation>(
            "failed to begin lobby invitation");
    }
    LobbyInfo info;
    if (!_impl->loadLobby(request.lobbyId, info)) {
        const int exists = _impl->rowExists(
            "ay_online_lobbies", "lobby_id", request.lobbyId);
        _impl->rollback();
        if (exists == 0) {
            return OnlineServiceResult<LobbyInvitation>::failure(
                OnlineServiceError::NotFound, "lobby not found");
        }
        return _impl->databaseFailure<LobbyInvitation>(
            "failed to load lobby for invitation");
    }
    if (info.ownerPeerId != request.actorPeerId) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInvitation>::failure(
            OnlineServiceError::Unauthorized,
            "only the lobby owner may create invitations");
    }
    if (info.revision != request.expectedRevision) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInvitation>::failure(
            OnlineServiceError::Conflict, "lobby revision changed");
    }
    if (info.state != LobbyState::Open) {
        _impl->rollback();
        return OnlineServiceResult<LobbyInvitation>::failure(
            OnlineServiceError::Closed, "lobby is not open");
    }
    LobbyInvitation invitation;
    invitation.lobbyId = request.lobbyId;
    invitation.token = randomToken();
    invitation.expiresAtUnixSeconds = _impl->now() + request.lifetimeSeconds;
    invitation.remainingUses = request.maxUses;
    const auto digest = tokenDigest(invitation.token);
    Statement insert(_impl->database,
        "INSERT INTO ay_online_lobby_invitations(lobby_id,token_hash,expires,"
        "remaining_uses) VALUES(?1,?2,?3,?4)");
    if (!invitation.isValid() || !insert ||
        !bindU64(insert.get(), 1, request.lobbyId) ||
        sqlite3_bind_blob(insert.get(), 2, digest.data(),
            static_cast<int>(digest.size()), SQLITE_TRANSIENT) != SQLITE_OK ||
        !bindU64(insert.get(), 3, invitation.expiresAtUnixSeconds) ||
        sqlite3_bind_int(insert.get(), 4, invitation.remainingUses) != SQLITE_OK ||
        sqlite3_step(insert.get()) != SQLITE_DONE || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyInvitation>(
            "failed to persist lobby invitation");
    }
    return OnlineServiceResult<LobbyInvitation>::success(
        std::move(invitation));
}

OnlineServiceResult<LobbyInfo> SqliteOnlineServices::getLobby(LobbyId lobbyId) {
    if (lobbyId == 0) return OnlineServiceResult<LobbyInfo>::failure(
        OnlineServiceError::InvalidRequest, "invalid durable lobby id");
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork()) {
        return _impl->databaseFailure<LobbyInfo>("failed to query lobby");
    }
    LobbyInfo info;
    if (!_impl->loadLobby(lobbyId, info)) {
        const int exists = _impl->rowExists(
            "ay_online_lobbies", "lobby_id", lobbyId);
        if (exists == 0) return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
        return _impl->databaseFailure<LobbyInfo>("failed to load lobby");
    }
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyLaunchResult> SqliteOnlineServices::launchLobbyP2P(
    const LaunchLobbyRequest& request) {
    if (request.lobbyId == 0 || !request.actorPeerId.isValid() ||
        request.expectedRevision == 0 || request.virtualPort == 0) {
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::InvalidRequest, "invalid durable lobby launch");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<LobbyLaunchResult>(
            "failed to begin durable lobby launch");
    }
    LobbyInfo snapshot;
    if (!_impl->loadLobby(request.lobbyId, snapshot)) {
        const int exists = _impl->rowExists(
            "ay_online_lobbies", "lobby_id", request.lobbyId);
        _impl->rollback();
        if (exists == 0) return OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::NotFound, "lobby not found");
        return _impl->databaseFailure<LobbyLaunchResult>("failed to load lobby");
    }
    if (snapshot.ownerPeerId != request.actorPeerId) {
        _impl->rollback();
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::Unauthorized, "only the lobby owner may launch");
    }
    if (snapshot.revision != request.expectedRevision ||
        snapshot.state != LobbyState::Open) {
        _impl->rollback();
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::Conflict, "lobby revision or state changed");
    }
    const std::string claim = randomToken();
    const uint64_t claimExpiry = _impl->now() +
                                  _impl->config.operationClaimSeconds;
    Statement claimLobby(_impl->database,
        "UPDATE ay_online_lobbies SET state=?1,revision=revision+1,"
        "launch_claim=?2,launch_expires=?3 WHERE lobby_id=?4 AND revision=?5 "
        "AND state=?6");
    if (claim.empty() || !claimLobby ||
        sqlite3_bind_int(claimLobby.get(), 1,
            static_cast<int>(LobbyState::Launching)) != SQLITE_OK ||
        !bindText(claimLobby.get(), 2, claim) ||
        !bindU64(claimLobby.get(), 3, claimExpiry) ||
        !bindU64(claimLobby.get(), 4, request.lobbyId) ||
        !bindU64(claimLobby.get(), 5, request.expectedRevision) ||
        sqlite3_bind_int(claimLobby.get(), 6,
            static_cast<int>(LobbyState::Open)) != SQLITE_OK ||
        sqlite3_step(claimLobby.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<LobbyLaunchResult>(
            "failed to claim durable lobby launch");
    }
    const uint64_t launchRevision = request.expectedRevision + 1;
    auto reopen = [&] {
        if (!_impl->begin()) return false;
        Statement update(_impl->database,
            "UPDATE ay_online_lobbies SET state=?1,revision=revision+1,"
            "launch_claim='',launch_expires=0 WHERE lobby_id=?2 AND state=?3 "
            "AND revision=?4 AND launch_claim=?5");
        const bool changed = update &&
            sqlite3_bind_int(update.get(), 1,
                static_cast<int>(LobbyState::Open)) == SQLITE_OK &&
            bindU64(update.get(), 2, request.lobbyId) &&
            sqlite3_bind_int(update.get(), 3,
                static_cast<int>(LobbyState::Launching)) == SQLITE_OK &&
            bindU64(update.get(), 4, launchRevision) &&
            bindText(update.get(), 5, claim) &&
            sqlite3_step(update.get()) == SQLITE_DONE &&
            sqlite3_changes(_impl->database) == 1;
        if (!changed || !_impl->commit()) {
            _impl->rollback();
            return false;
        }
        return true;
    };
    if (!_impl->p2pSessions) {
        (void)reopen();
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::BackendUnavailable,
            "P2P session backend is not configured");
    }
    auto host = _impl->p2pSessions->createSession(
        {snapshot.ownerPeerId, request.virtualPort, snapshot.capacity});
    if (!host) {
        (void)reopen();
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            mapSessionError(host.error), host.message);
    }
    std::vector<P2PSessionGrant> grants{host.value};
    for (const PeerId& peer : snapshot.members) {
        if (peer == snapshot.ownerPeerId) continue;
        auto joined = _impl->p2pSessions->joinSession(
            {host.value.session.sessionId, peer});
        if (!joined) {
            (void)_impl->p2pSessions->leaveSession({host.value.member});
            (void)reopen();
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                mapSessionError(joined.error), joined.message);
        }
        grants.push_back(std::move(joined.value));
    }
    if (!_impl->begin()) {
        (void)_impl->p2pSessions->leaveSession({host.value.member});
        (void)reopen();
        return _impl->databaseFailure<LobbyLaunchResult>(
            "failed to finalize lobby launch");
    }
    Statement finish(_impl->database,
        "UPDATE ay_online_lobbies SET state=?1,session_id=?2,revision=revision+1,"
        "launch_claim='',launch_expires=0 WHERE lobby_id=?3 AND state=?4 "
        "AND revision=?5 AND launch_claim=?6");
    const bool changed = finish &&
        sqlite3_bind_int(finish.get(), 1,
            static_cast<int>(LobbyState::InSession)) == SQLITE_OK &&
        bindOpaqueU64(finish.get(), 2, host.value.session.sessionId) &&
        bindU64(finish.get(), 3, request.lobbyId) &&
        sqlite3_bind_int(finish.get(), 4,
            static_cast<int>(LobbyState::Launching)) == SQLITE_OK &&
        bindU64(finish.get(), 5, launchRevision) &&
        bindText(finish.get(), 6, claim) &&
        sqlite3_step(finish.get()) == SQLITE_DONE &&
        sqlite3_changes(_impl->database) == 1;
    LobbyInfo launched;
    if (!changed || !_impl->loadLobby(request.lobbyId, launched) ||
        !_impl->commit()) {
        _impl->rollback();
        (void)_impl->p2pSessions->leaveSession({host.value.member});
        (void)reopen();
        return _impl->databaseFailure<LobbyLaunchResult>(
            "failed to persist lobby launch result");
    }
    LobbyLaunchResult result;
    result.lobby = std::move(launched);
    result.memberGrants = std::move(grants);
    return OnlineServiceResult<LobbyLaunchResult>::success(std::move(result));
}

OnlineServiceResult<DedicatedServerGrant> SqliteOnlineServices::registerServer(
    const DedicatedServerRegistration& request) {
    if (request.instanceName.empty() ||
        request.instanceName.size() > kMaxNameBytes ||
        !validKey(request.region) || !validKey(request.buildId) ||
        request.address.empty() || request.address.size() > kMaxAddressBytes ||
        request.port == 0 || request.capacity == 0 ||
        request.capacity > _impl->config.maxDedicatedServerCapacity) {
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable server registration");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerGrant>(
            "failed to begin server registration");
    }
    Statement duplicate(_impl->database,
        "SELECT 1 FROM ay_online_servers WHERE instance_name=?1");
    if (!duplicate || !bindText(duplicate.get(), 1, request.instanceName)) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerGrant>(
            "failed to check server registration");
    }
    const int duplicateResult = sqlite3_step(duplicate.get());
    if (duplicateResult == SQLITE_ROW) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedServerGrant>(
                "failed to finish server registration");
        }
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::Conflict,
            "server instance is already registered");
    }
    Statement count(_impl->database, "SELECT COUNT(*) FROM ay_online_servers");
    if (duplicateResult != SQLITE_DONE || !count ||
        sqlite3_step(count.get()) != SQLITE_ROW) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerGrant>(
            "failed to count dedicated servers");
    }
    if (static_cast<uint64_t>(sqlite3_column_int64(count.get(), 0)) >=
        _impl->config.maxDedicatedServers) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedServerGrant>(
                "failed to finish server registration");
        }
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::Full,
            "durable server directory is full");
    }
    const DedicatedServerId id = _impl->allocateId(
        "ay_online_servers", "server_id");
    const std::string token = randomToken();
    std::vector<uint8_t> sealed;
    if (id == 0 || token.empty() ||
        !sealSecret(_impl->config.storageKey, secretContext("server", id),
                    reinterpret_cast<const uint8_t*>(token.data()), token.size(),
                    sealed)) {
        _impl->rollback();
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::InternalError,
            "failed to create durable server credential");
    }
    const uint64_t expiry = _impl->now() +
                            _impl->config.dedicatedLeaseSeconds;
    Statement insert(_impl->database,
        "INSERT INTO ay_online_servers(server_id,instance_name,region,build_id,"
        "address,port,capacity,reserved_players,draining,lease_expires,token) "
        "VALUES(?1,?2,?3,?4,?5,?6,?7,0,0,?8,?9)");
    const bool inserted = insert && bindU64(insert.get(), 1, id) &&
        bindText(insert.get(), 2, request.instanceName) &&
        bindText(insert.get(), 3, request.region) &&
        bindText(insert.get(), 4, request.buildId) &&
        bindText(insert.get(), 5, request.address) &&
        sqlite3_bind_int(insert.get(), 6, request.port) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 7, request.capacity) == SQLITE_OK &&
        bindU64(insert.get(), 8, expiry) && bindBlob(insert.get(), 9, sealed) &&
        sqlite3_step(insert.get()) == SQLITE_DONE;
    Impl::ServerRecord record;
    if (!inserted || !_impl->loadServer(id, record) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerGrant>(
            "failed to persist dedicated server");
    }
    DedicatedServerGrant grant;
    grant.server = std::move(record.info);
    grant.credential.serverId = id;
    grant.credential.token = token;
    return OnlineServiceResult<DedicatedServerGrant>::success(std::move(grant));
}

OnlineServiceResult<DedicatedServerInfo> SqliteOnlineServices::heartbeatServer(
    const DedicatedServerCredential& credential) {
    if (!credential.isValid()) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable server credential");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerInfo>(
            "failed to begin server heartbeat");
    }
    Impl::ServerRecord record;
    if (!_impl->loadServer(credential.serverId, record)) {
        const int exists = _impl->rowExists(
            "ay_online_servers", "server_id", credential.serverId);
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedServerInfo>(
                "failed to expire dedicated leases");
        }
        if (exists == 0) return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::NotFound, "server lease not found");
        return _impl->databaseFailure<DedicatedServerInfo>(
            "failed to load server credential");
    }
    if (!tokenEqual(record.token, credential.token)) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedServerInfo>(
                "failed to finish server heartbeat");
        }
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::Unauthorized,
            "invalid server credential");
    }
    const uint64_t expiry = _impl->now() +
                            _impl->config.dedicatedLeaseSeconds;
    Statement update(_impl->database,
        "UPDATE ay_online_servers SET lease_expires=?1 WHERE server_id=?2");
    if (!update || !bindU64(update.get(), 1, expiry) ||
        !bindU64(update.get(), 2, credential.serverId) ||
        sqlite3_step(update.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 ||
        !_impl->loadServer(credential.serverId, record) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerInfo>(
            "failed to persist server heartbeat");
    }
    return OnlineServiceResult<DedicatedServerInfo>::success(
        std::move(record.info));
}

OnlineServiceResult<DedicatedServerInfo>
SqliteOnlineServices::setServerDraining(
    const DedicatedServerCredential& credential, bool draining) {
    if (!credential.isValid()) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable server credential");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerInfo>(
            "failed to begin server drain update");
    }
    Impl::ServerRecord record;
    if (!_impl->loadServer(credential.serverId, record)) {
        const int exists = _impl->rowExists(
            "ay_online_servers", "server_id", credential.serverId);
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedServerInfo>(
                "failed to expire dedicated leases");
        }
        if (exists == 0) return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::NotFound, "server not found");
        return _impl->databaseFailure<DedicatedServerInfo>(
            "failed to load server credential");
    }
    if (!tokenEqual(record.token, credential.token)) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedServerInfo>(
                "failed to finish server drain update");
        }
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::Unauthorized,
            "invalid server credential");
    }
    Statement update(_impl->database,
        "UPDATE ay_online_servers SET draining=?1 WHERE server_id=?2");
    if (!update || sqlite3_bind_int(update.get(), 1, draining ? 1 : 0) != SQLITE_OK ||
        !bindU64(update.get(), 2, credential.serverId) ||
        sqlite3_step(update.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 ||
        !_impl->loadServer(credential.serverId, record) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedServerInfo>(
            "failed to persist server drain state");
    }
    return OnlineServiceResult<DedicatedServerInfo>::success(
        std::move(record.info));
}

OnlineServiceResult<SessionServiceEmpty>
SqliteOnlineServices::unregisterServer(
    const DedicatedServerCredential& credential) {
    if (!credential.isValid()) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable server credential");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to begin server unregister");
    }
    Impl::ServerRecord record;
    if (!_impl->loadServer(credential.serverId, record)) {
        const int exists = _impl->rowExists(
            "ay_online_servers", "server_id", credential.serverId);
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<SessionServiceEmpty>(
                "failed to expire dedicated leases");
        }
        if (exists == 0) return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::NotFound, "server not found");
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to load server credential");
    }
    if (!tokenEqual(record.token, credential.token)) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<SessionServiceEmpty>(
                "failed to finish server unregister");
        }
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Unauthorized,
            "invalid server credential");
    }
    if (record.info.reservedPlayers != 0) {
        Statement drain(_impl->database,
            "UPDATE ay_online_servers SET draining=1 WHERE server_id=?1");
        if (!drain || !bindU64(drain.get(), 1, credential.serverId) ||
            sqlite3_step(drain.get()) != SQLITE_DONE || !_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<SessionServiceEmpty>(
                "failed to persist draining server");
        }
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Conflict,
            "server is draining until allocations are released");
    }
    Statement remove(_impl->database,
        "DELETE FROM ay_online_servers WHERE server_id=?1");
    if (!remove || !bindU64(remove.get(), 1, credential.serverId) ||
        sqlite3_step(remove.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to unregister dedicated server");
    }
    return OnlineServiceResult<SessionServiceEmpty>::success({});
}

OnlineServiceResult<DedicatedAllocation>
SqliteOnlineServices::allocateServer(
    const DedicatedAllocationRequest& request) {
    if (!validKey(request.region) || !validKey(request.buildId) ||
        request.playerCount == 0 ||
        request.playerCount > _impl->config.maxMatchPlayers) {
        return OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable allocation request");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedAllocation>(
            "failed to begin dedicated allocation");
    }
    Statement select(_impl->database,
        "SELECT server_id,address,port FROM ay_online_servers WHERE draining=0 "
        "AND region=?1 AND build_id=?2 AND reserved_players+?3<=capacity "
        "ORDER BY (1.0*reserved_players)/capacity,server_id LIMIT 1");
    if (!select || !bindText(select.get(), 1, request.region) ||
        !bindText(select.get(), 2, request.buildId) ||
        sqlite3_bind_int(select.get(), 3, request.playerCount) != SQLITE_OK) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedAllocation>(
            "failed to select dedicated capacity");
    }
    const int selected = sqlite3_step(select.get());
    if (selected == SQLITE_DONE) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<DedicatedAllocation>(
                "failed to finish dedicated allocation");
        }
        return OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::NoCapacity,
            "no eligible durable dedicated server");
    }
    if (selected != SQLITE_ROW) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedAllocation>(
            "failed to read dedicated capacity");
    }
    DedicatedAllocation allocation;
    allocation.serverId = static_cast<uint64_t>(
        sqlite3_column_int64(select.get(), 0));
    allocation.address = columnText(select.get(), 1);
    allocation.port = static_cast<uint16_t>(sqlite3_column_int(select.get(), 2));
    allocation.allocationId = _impl->allocateId(
        "ay_online_allocations", "allocation_id");
    allocation.playerCount = request.playerCount;
    allocation.reservationToken = randomToken();
    allocation.expiresAtUnixSeconds = _impl->now() +
        _impl->config.allocationLifetimeSeconds;
    std::vector<uint8_t> sealed;
    if (!allocation.isValid() ||
        !sealSecret(_impl->config.storageKey,
                    secretContext("allocation", allocation.allocationId),
                    reinterpret_cast<const uint8_t*>(
                        allocation.reservationToken.data()),
                    allocation.reservationToken.size(), sealed)) {
        _impl->rollback();
        return OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::InternalError,
            "failed to create durable allocation credential");
    }
    Statement insert(_impl->database,
        "INSERT INTO ay_online_allocations(allocation_id,server_id,address,port,"
        "player_count,expires,token) VALUES(?1,?2,?3,?4,?5,?6,?7)");
    Statement reserve(_impl->database,
        "UPDATE ay_online_servers SET reserved_players=reserved_players+?1 "
        "WHERE server_id=?2 AND draining=0 AND reserved_players+?1<=capacity");
    const bool persisted = insert && reserve &&
        bindU64(insert.get(), 1, allocation.allocationId) &&
        bindU64(insert.get(), 2, allocation.serverId) &&
        bindText(insert.get(), 3, allocation.address) &&
        sqlite3_bind_int(insert.get(), 4, allocation.port) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 5, allocation.playerCount) == SQLITE_OK &&
        bindU64(insert.get(), 6, allocation.expiresAtUnixSeconds) &&
        bindBlob(insert.get(), 7, sealed) &&
        sqlite3_step(insert.get()) == SQLITE_DONE &&
        sqlite3_bind_int(reserve.get(), 1, allocation.playerCount) == SQLITE_OK &&
        bindU64(reserve.get(), 2, allocation.serverId) &&
        sqlite3_step(reserve.get()) == SQLITE_DONE &&
        sqlite3_changes(_impl->database) == 1;
    if (!persisted || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<DedicatedAllocation>(
            "failed to persist dedicated allocation");
    }
    return OnlineServiceResult<DedicatedAllocation>::success(
        std::move(allocation));
}

OnlineServiceResult<SessionServiceEmpty>
SqliteOnlineServices::releaseAllocation(
    DedicatedAllocationId allocationId,
    const std::string& reservationToken) {
    if (allocationId == 0 || reservationToken.size() != 64) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable allocation credential");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to begin allocation release");
    }
    Statement select(_impl->database,
        "SELECT server_id,player_count,token FROM ay_online_allocations "
        "WHERE allocation_id=?1");
    if (!select || !bindU64(select.get(), 1, allocationId)) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to query allocation");
    }
    const int selected = sqlite3_step(select.get());
    if (selected == SQLITE_DONE) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<SessionServiceEmpty>(
                "failed to finish allocation release");
        }
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::NotFound, "allocation not found");
    }
    if (selected != SQLITE_ROW) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to read allocation");
    }
    const DedicatedServerId serverId = static_cast<uint64_t>(
        sqlite3_column_int64(select.get(), 0));
    const uint16_t players = static_cast<uint16_t>(
        sqlite3_column_int(select.get(), 1));
    std::vector<uint8_t> plain;
    const auto sealed = columnBlob(select.get(), 2);
    if (!openSecret(_impl->config.storageKey,
                    secretContext("allocation", allocationId), sealed, plain)) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to decrypt allocation credential");
    }
    const std::string stored(reinterpret_cast<const char*>(plain.data()), plain.size());
    if (!plain.empty()) sodium_memzero(plain.data(), plain.size());
    if (!tokenEqual(stored, reservationToken)) {
        if (!_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<SessionServiceEmpty>(
                "failed to finish allocation release");
        }
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Unauthorized,
            "invalid allocation credential");
    }
    Statement remove(_impl->database,
        "DELETE FROM ay_online_allocations WHERE allocation_id=?1");
    Statement release(_impl->database,
        "UPDATE ay_online_servers SET reserved_players=MAX(0,reserved_players-?1) "
        "WHERE server_id=?2");
    const bool removed = remove && release &&
        bindU64(remove.get(), 1, allocationId) &&
        sqlite3_step(remove.get()) == SQLITE_DONE &&
        sqlite3_changes(_impl->database) == 1 &&
        sqlite3_bind_int(release.get(), 1, players) == SQLITE_OK &&
        bindU64(release.get(), 2, serverId) &&
        sqlite3_step(release.get()) == SQLITE_DONE;
    if (!removed || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>(
            "failed to persist allocation release");
    }
    return OnlineServiceResult<SessionServiceEmpty>::success({});
}

OnlineServiceResult<std::vector<DedicatedServerInfo>>
SqliteOnlineServices::listServers() {
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin() || !_impl->expireDedicated()) {
        _impl->rollback();
        return _impl->databaseFailure<std::vector<DedicatedServerInfo>>(
            "failed to query dedicated servers");
    }
    Statement statement(_impl->database,
        "SELECT server_id FROM ay_online_servers ORDER BY server_id");
    if (!statement) {
        _impl->rollback();
        return _impl->databaseFailure<std::vector<DedicatedServerInfo>>(
            "failed to prepare server query");
    }
    std::vector<DedicatedServerInfo> result;
    int step = SQLITE_ROW;
    while ((step = sqlite3_step(statement.get())) == SQLITE_ROW) {
        Impl::ServerRecord record;
        const DedicatedServerId id = static_cast<uint64_t>(
            sqlite3_column_int64(statement.get(), 0));
        if (!_impl->loadServer(id, record)) {
            _impl->rollback();
            return _impl->databaseFailure<std::vector<DedicatedServerInfo>>(
                "invalid durable server record");
        }
        result.push_back(std::move(record.info));
    }
    if (step != SQLITE_DONE || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<std::vector<DedicatedServerInfo>>(
            "failed to read dedicated servers");
    }
    return OnlineServiceResult<std::vector<DedicatedServerInfo>>::success(
        std::move(result));
}

OnlineServiceResult<MatchTicketInfo> SqliteOnlineServices::enqueueMatch(
    const MatchmakingRequest& request) {
    MatchmakingRequest normalized = request;
    if (normalized.minimumPlayers == 0) {
        normalized.minimumPlayers = normalized.targetPlayers;
    }
    if (!normalized.allowBackfill) {
        normalized.minimumPlayers = normalized.targetPlayers;
    }
    if (!validKey(normalized.queue) || !validKey(normalized.region) ||
        !validKey(normalized.buildId) || !normalized.content.isValid() ||
        normalized.targetPlayers < 2 || normalized.minimumPlayers < 2 ||
        normalized.minimumPlayers > normalized.targetPlayers ||
        normalized.targetPlayers > _impl->config.maxMatchPlayers ||
        normalized.virtualPort == 0 || normalized.teamCount == 0 ||
        normalized.teamCount > normalized.targetPlayers ||
        (normalized.maxPingMs != 0 &&
         normalized.estimatedPingMs > normalized.maxPingMs) ||
        static_cast<uint8_t>(normalized.topology) >
            static_cast<uint8_t>(MatchTopology::Any)) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable matchmaking request");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to begin match enqueue");
    }
    if (normalized.sourceLobbyId != 0) {
        LobbyInfo party;
        if (!_impl->loadLobby(normalized.sourceLobbyId, party)) {
            const int exists = _impl->rowExists(
                "ay_online_lobbies", "lobby_id", normalized.sourceLobbyId);
            _impl->rollback();
            if (exists == 0) {
                return OnlineServiceResult<MatchTicketInfo>::failure(
                    OnlineServiceError::NotFound,
                    "matchmaking lobby not found");
            }
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to load matchmaking lobby");
        }
        if (!normalized.partyLeaderPeerId.isValid() ||
            party.ownerPeerId != normalized.partyLeaderPeerId) {
            _impl->rollback();
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::Unauthorized,
                "only the lobby leader may queue the party");
        }
        if (normalized.sourceLobbyRevision == 0 ||
            party.revision != normalized.sourceLobbyRevision) {
            _impl->rollback();
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::Conflict, "lobby revision changed");
        }
        if (party.state != LobbyState::Open) {
            _impl->rollback();
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::Closed, "lobby is not open");
        }
        normalized.partyMembers = party.members;
        normalized.region = party.region;
        normalized.buildId = party.buildId;
        normalized.content = party.content;
    }
    if (!validParty(normalized.partyMembers, normalized.targetPlayers)) {
        _impl->rollback();
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable matchmaking party");
    }
    Statement count(_impl->database, "SELECT COUNT(*) FROM ay_online_tickets");
    if (!count || sqlite3_step(count.get()) != SQLITE_ROW) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to count match tickets");
    }
    if (static_cast<uint64_t>(sqlite3_column_int64(count.get(), 0)) >=
        _impl->config.maxMatchTickets) {
        _impl->rollback();
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Full,
            "durable matchmaking queue is full");
    }
    for (const PeerId& peer : normalized.partyMembers) {
        Statement active(_impl->database,
            "SELECT 1 FROM ay_online_ticket_members m JOIN ay_online_tickets t "
            "ON t.ticket_id=m.ticket_id WHERE m.peer_id=?1 "
            "AND t.state IN (?2,?3,?4) "
            "LIMIT 1");
        if (!active || !bindText(active.get(), 1, peer.value) ||
            sqlite3_bind_int(active.get(), 2,
                static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK ||
            sqlite3_bind_int(active.get(), 3,
                static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
            sqlite3_bind_int(active.get(), 4,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to check active match tickets");
        }
        const int activeResult = sqlite3_step(active.get());
        if (activeResult == SQLITE_ROW) {
            _impl->rollback();
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::Conflict,
                "a party member already has an active match ticket");
        }
        if (activeResult != SQLITE_DONE) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to read active match tickets");
        }
    }
    const MatchTicketId id = _impl->allocateId(
        "ay_online_tickets", "ticket_id");
    Statement insert(_impl->database,
        "INSERT INTO ay_online_tickets(ticket_id,state,queue_name,region,build_id,"
        "content_id,content_version,content_seed,topology,target_players,"
        "minimum_players,virtual_port,source_lobby_id,source_lobby_revision,"
        "party_leader,estimated_ping_ms,max_ping_ms,skill_rating,skill_tolerance,"
        "team_count,allow_backfill,require_acceptance,created_at) "
        "VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,"
        "?16,?17,?18,?19,?20,?21,?22,?23)");
    const bool inserted = id != 0 && insert && bindU64(insert.get(), 1, id) &&
        sqlite3_bind_int(insert.get(), 2,
            static_cast<int>(MatchTicketState::Queued)) == SQLITE_OK &&
        bindText(insert.get(), 3, normalized.queue) &&
        bindText(insert.get(), 4, normalized.region) &&
        bindText(insert.get(), 5, normalized.buildId) &&
        bindText(insert.get(), 6, normalized.content.contentId) &&
        bindText(insert.get(), 7, normalized.content.contentVersion) &&
        bindU64(insert.get(), 8, normalized.content.contentSeed) &&
        sqlite3_bind_int(insert.get(), 9,
            static_cast<int>(normalized.topology)) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 10, normalized.targetPlayers) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 11, normalized.minimumPlayers) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 12, normalized.virtualPort) == SQLITE_OK &&
        bindU64(insert.get(), 13, normalized.sourceLobbyId) &&
        bindU64(insert.get(), 14, normalized.sourceLobbyRevision) &&
        bindText(insert.get(), 15, normalized.partyLeaderPeerId.value) &&
        sqlite3_bind_int(insert.get(), 16, normalized.estimatedPingMs) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 17, normalized.maxPingMs) == SQLITE_OK &&
        bindU64(insert.get(), 18, normalized.skillRating) &&
        bindU64(insert.get(), 19, normalized.skillTolerance) &&
        sqlite3_bind_int(insert.get(), 20, normalized.teamCount) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 21, normalized.allowBackfill ? 1 : 0) == SQLITE_OK &&
        sqlite3_bind_int(insert.get(), 22, normalized.requireAcceptance ? 1 : 0) == SQLITE_OK &&
        bindU64(insert.get(), 23, _impl->now()) &&
        sqlite3_step(insert.get()) == SQLITE_DONE;
    if (!inserted) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to persist match ticket");
    }
    for (size_t index = 0; index < normalized.partyMembers.size(); ++index) {
        Statement member(_impl->database,
            "INSERT INTO ay_online_ticket_members(ticket_id,ordinal,peer_id) "
            "VALUES(?1,?2,?3)");
        if (!member || !bindU64(member.get(), 1, id) ||
            sqlite3_bind_int64(member.get(), 2,
                static_cast<sqlite3_int64>(index)) != SQLITE_OK ||
            !bindText(member.get(), 3, normalized.partyMembers[index].value) ||
            sqlite3_step(member.get()) != SQLITE_DONE) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to persist match party");
        }
    }
    MatchTicketInfo info;
    if (!_impl->loadTicket(id, info) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to finish match enqueue");
    }
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

OnlineServiceResult<MatchTicketInfo> SqliteOnlineServices::getMatch(
    MatchTicketId ticketId, const PeerId& authenticatedPeer) {
    if (ticketId == 0 || !authenticatedPeer.isValid()) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable match query");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork()) {
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to query durable match");
    }
    if (!_impl->isTicketMember(ticketId, authenticatedPeer)) {
        const int exists = _impl->rowExists(
            "ay_online_tickets", "ticket_id", ticketId);
        if (exists == 0) return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::NotFound, "match ticket not found");
        if (exists == 1) return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized, "peer does not own this ticket");
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to authorize match query");
    }
    MatchTicketInfo info;
    if (!_impl->loadTicket(ticketId, info)) {
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to load durable match ticket");
    }
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

OnlineServiceResult<MatchTicketInfo> SqliteOnlineServices::cancelMatch(
    MatchTicketId ticketId, const PeerId& authenticatedPeer) {
    if (ticketId == 0 || !authenticatedPeer.isValid()) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable match cancellation");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to begin match cancellation");
    }
    const int exists = _impl->rowExists(
        "ay_online_tickets", "ticket_id", ticketId);
    if (exists == 0) {
        _impl->rollback();
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::NotFound, "match ticket not found");
    }
    if (exists < 0) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to query match ticket");
    }
    if (!_impl->isTicketMember(ticketId, authenticatedPeer)) {
        _impl->rollback();
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized, "peer does not own this ticket");
    }
    MatchTicketInfo current;
    if (!_impl->loadTicket(ticketId, current)) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to load match ticket");
    }
    if (current.state == MatchTicketState::AwaitingAcceptance) {
        const uint64_t matchId = current.assignment.matchId;
        Statement requeue(_impl->database,
            "UPDATE ay_online_tickets SET state=?1,match_id=0,placements='[]',"
            "accepted_members='[]',acceptance_expires=0,"
            "failure='another party cancelled; ticket requeued' "
            "WHERE match_id=?2 AND state=?3 AND ticket_id!=?4");
        Statement cancel(_impl->database,
            "UPDATE ay_online_tickets SET state=?1,match_id=0,placements='[]',"
            "accepted_members='[]',acceptance_expires=0,completed_at=?2,"
            "failure='match acceptance cancelled' WHERE ticket_id=?3 AND "
            "state=?4 AND match_id=?5");
        if (!requeue || !cancel || sqlite3_bind_int(requeue.get(), 1,
                static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK ||
            !bindU64(requeue.get(), 2, matchId) ||
            sqlite3_bind_int(requeue.get(), 3,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK ||
            !bindU64(requeue.get(), 4, ticketId) ||
            sqlite3_step(requeue.get()) != SQLITE_DONE ||
            sqlite3_bind_int(cancel.get(), 1,
                static_cast<int>(MatchTicketState::Cancelled)) != SQLITE_OK ||
            !bindU64(cancel.get(), 2, _impl->now()) ||
            !bindU64(cancel.get(), 3, ticketId) ||
            sqlite3_bind_int(cancel.get(), 4,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK ||
            !bindU64(cancel.get(), 5, matchId) ||
            sqlite3_step(cancel.get()) != SQLITE_DONE ||
            sqlite3_changes(_impl->database) != 1 ||
            !_impl->loadTicket(ticketId, current) || !_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to cancel match acceptance");
        }
        return OnlineServiceResult<MatchTicketInfo>::success(
            std::move(current));
    }
    if (current.state != MatchTicketState::Queued) {
        _impl->rollback();
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Conflict,
            "match ticket is no longer queued");
    }
    Statement update(_impl->database,
        "UPDATE ay_online_tickets SET state=?1,completed_at=?2 WHERE ticket_id=?3 "
        "AND state=?4");
    if (!update || sqlite3_bind_int(update.get(), 1,
            static_cast<int>(MatchTicketState::Cancelled)) != SQLITE_OK ||
        !bindU64(update.get(), 2, _impl->now()) ||
        !bindU64(update.get(), 3, ticketId) ||
        sqlite3_bind_int(update.get(), 4,
            static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK ||
        sqlite3_step(update.get()) != SQLITE_DONE ||
        sqlite3_changes(_impl->database) != 1 ||
        !_impl->loadTicket(ticketId, current) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to persist match cancellation");
    }
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(current));
}

OnlineServiceResult<MatchTicketInfo> SqliteOnlineServices::respondToMatch(
    const MatchAcceptanceRequest& request) {
    if (request.ticketId == 0 || !request.authenticatedPeer.isValid()) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest,
            "invalid durable match response");
    }
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->recoverExpiredWork() || !_impl->begin()) {
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to begin match response");
    }
    if (!_impl->isTicketMember(request.ticketId,
                               request.authenticatedPeer)) {
        const int exists = _impl->rowExists(
            "ay_online_tickets", "ticket_id", request.ticketId);
        _impl->rollback();
        if (exists == 0) {
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::NotFound, "match ticket not found");
        }
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized, "peer does not own this ticket");
    }
    MatchTicketInfo current;
    if (!_impl->loadTicket(request.ticketId, current)) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to load match response ticket");
    }
    if (current.state != MatchTicketState::AwaitingAcceptance ||
        current.assignment.matchId == 0) {
        _impl->rollback();
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Conflict,
            "match ticket is not awaiting acceptance");
    }
    const uint64_t matchId = current.assignment.matchId;
    if (!request.accept) {
        Statement requeue(_impl->database,
            "UPDATE ay_online_tickets SET state=?1,match_id=0,placements='[]',"
            "accepted_members='[]',acceptance_expires=0,"
            "failure='another party declined; ticket requeued' "
            "WHERE match_id=?2 AND state=?3 AND ticket_id!=?4");
        Statement decline(_impl->database,
            "UPDATE ay_online_tickets SET state=?1,match_id=0,placements='[]',"
            "accepted_members='[]',acceptance_expires=0,completed_at=?2,"
            "failure='a party member declined the match' WHERE ticket_id=?3 "
            "AND state=?4 AND match_id=?5");
        if (!requeue || !decline || sqlite3_bind_int(requeue.get(), 1,
                static_cast<int>(MatchTicketState::Queued)) != SQLITE_OK ||
            !bindU64(requeue.get(), 2, matchId) ||
            sqlite3_bind_int(requeue.get(), 3,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK ||
            !bindU64(requeue.get(), 4, request.ticketId) ||
            sqlite3_step(requeue.get()) != SQLITE_DONE ||
            sqlite3_bind_int(decline.get(), 1,
                static_cast<int>(MatchTicketState::Cancelled)) != SQLITE_OK ||
            !bindU64(decline.get(), 2, _impl->now()) ||
            !bindU64(decline.get(), 3, request.ticketId) ||
            sqlite3_bind_int(decline.get(), 4,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK ||
            !bindU64(decline.get(), 5, matchId) ||
            sqlite3_step(decline.get()) != SQLITE_DONE ||
            sqlite3_changes(_impl->database) != 1 ||
            !_impl->loadTicket(request.ticketId, current) || !_impl->commit()) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to persist match decline");
        }
        return OnlineServiceResult<MatchTicketInfo>::success(
            std::move(current));
    }

    if (!containsPeer(current.acceptedMembers, request.authenticatedPeer)) {
        current.acceptedMembers.push_back(request.authenticatedPeer);
        Statement accept(_impl->database,
            "UPDATE ay_online_tickets SET accepted_members=?1 WHERE ticket_id=?2 "
            "AND state=?3 AND match_id=?4");
        if (!accept || !bindText(accept.get(), 1,
                peersToText(current.acceptedMembers)) ||
            !bindU64(accept.get(), 2, request.ticketId) ||
            sqlite3_bind_int(accept.get(), 3,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK ||
            !bindU64(accept.get(), 4, matchId) ||
            sqlite3_step(accept.get()) != SQLITE_DONE ||
            sqlite3_changes(_impl->database) != 1) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to persist match acceptance");
        }
    }

    bool allAccepted = true;
    Statement group(_impl->database,
        "SELECT ticket_id FROM ay_online_tickets WHERE match_id=?1 AND state=?2");
    if (!group || !bindU64(group.get(), 1, matchId) ||
        sqlite3_bind_int(group.get(), 2,
            static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to query match acceptance group");
    }
    int step = SQLITE_ROW;
    size_t tickets = 0;
    while ((step = sqlite3_step(group.get())) == SQLITE_ROW) {
        MatchTicketInfo member;
        const MatchTicketId id = static_cast<uint64_t>(
            sqlite3_column_int64(group.get(), 0));
        if (!_impl->loadTicket(id, member)) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to load match acceptance group");
        }
        if (id == request.ticketId) member.acceptedMembers = current.acceptedMembers;
        ++tickets;
        for (const PeerId& peer : member.request.partyMembers) {
            if (!containsPeer(member.acceptedMembers, peer)) allAccepted = false;
        }
    }
    if (step != SQLITE_DONE || tickets == 0) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to read match acceptance group");
    }
    if (allAccepted) {
        Statement ready(_impl->database,
            "UPDATE ay_online_tickets SET state=?1,acceptance_expires=0,failure='' "
            "WHERE match_id=?2 AND state=?3");
        if (!ready || sqlite3_bind_int(ready.get(), 1,
                static_cast<int>(MatchTicketState::Matching)) != SQLITE_OK ||
            !bindU64(ready.get(), 2, matchId) ||
            sqlite3_bind_int(ready.get(), 3,
                static_cast<int>(MatchTicketState::AwaitingAcceptance)) != SQLITE_OK ||
            sqlite3_step(ready.get()) != SQLITE_DONE ||
            static_cast<size_t>(sqlite3_changes(_impl->database)) != tickets) {
            _impl->rollback();
            return _impl->databaseFailure<MatchTicketInfo>(
                "failed to ready accepted match");
        }
    }
    if (!_impl->loadTicket(request.ticketId, current) || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<MatchTicketInfo>(
            "failed to finish match acceptance");
    }
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(current));
}

size_t SqliteOnlineServices::runMatchmaking(size_t maxMatches) {
    if (maxMatches == 0 || maxMatches > 1000) return 0;
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready) return 0;
    size_t processed = 0;
    while (processed < maxMatches) {
        std::vector<MatchTicketId> batch;
        std::vector<PeerId> peers;
        MatchmakingRequest request;
        std::string claim;
        uint64_t matchId = 0;
        std::vector<MatchPlayerPlacement> placements;
        const auto claimed = _impl->claimMatchBatch(
            batch, request, peers, claim, matchId, placements);
        if (claimed == Impl::ClaimResult::None) break;
        if (claimed == Impl::ClaimResult::Error) {
            _impl->setDatabaseError("failed to claim durable match batch");
            break;
        }

        const bool needsAcceptance = request.requireAcceptance && matchId == 0;
        if (matchId == 0) {
            for (unsigned attempt = 0; attempt < 8 && matchId == 0; ++attempt) {
                randombytes_buf(&matchId, sizeof(matchId));
                matchId &= static_cast<uint64_t>(
                    (std::numeric_limits<int64_t>::max)());
            }
        }
        if (matchId == 0) {
            (void)_impl->resetMatchBatch(batch, claim);
            _impl->setDatabaseError("failed to allocate durable match id");
            break;
        }
        if (needsAcceptance &&
            !_impl->stageMatchAcceptance(
                batch, claim, matchId, placements)) {
            (void)_impl->resetMatchBatch(batch, claim);
            _impl->setDatabaseError("failed to stage durable match acceptance");
            break;
        }
        if (needsAcceptance) {
            ++processed;
            continue;
        }

        MatchAssignment assignment;
        assignment.matchId = matchId;
        assignment.content = request.content;
        assignment.placements = placements;
        OnlineServiceError failure = OnlineServiceError::None;
        std::string failureMessage;
        if (request.topology == MatchTopology::Dedicated ||
            request.topology == MatchTopology::Any) {
            auto allocated = allocateServer({
                request.region, request.buildId,
                static_cast<uint16_t>(peers.size())});
            if (allocated) {
                assignment.topology = MatchTopology::Dedicated;
                assignment.dedicated = std::move(allocated.value);
            } else if (request.topology == MatchTopology::Dedicated) {
                failure = allocated.error;
                failureMessage = allocated.message;
            }
        }

        if (!assignment.dedicated.isValid() &&
            request.topology != MatchTopology::Dedicated) {
            if (!_impl->p2pSessions) {
                failure = OnlineServiceError::BackendUnavailable;
                failureMessage = "P2P session backend is not configured";
            } else {
                auto host = _impl->p2pSessions->createSession(
                    {peers.front(), request.virtualPort, request.targetPlayers});
                if (!host) {
                    failure = mapSessionError(host.error);
                    failureMessage = host.message;
                } else {
                    assignment.topology = MatchTopology::P2P;
                    assignment.p2pGrants.push_back(host.value);
                    for (size_t index = 1; index < peers.size(); ++index) {
                        auto joined = _impl->p2pSessions->joinSession(
                            {host.value.session.sessionId, peers[index]});
                        if (!joined) {
                            failure = mapSessionError(joined.error);
                            failureMessage = joined.message;
                            (void)_impl->p2pSessions->leaveSession(
                                {host.value.member});
                            assignment.p2pGrants.clear();
                            break;
                        }
                        assignment.p2pGrants.push_back(std::move(joined.value));
                    }
                }
            }
        }

        if (failure == OnlineServiceError::NoCapacity &&
            request.topology == MatchTopology::Dedicated) {
            if (!_impl->resetMatchBatch(batch, claim)) {
                _impl->setDatabaseError(
                    "failed to release durable match claim");
            }
            break;
        }

        if (!_impl->finalizeMatchBatch(
                batch, claim, assignment, failure, failureMessage)) {
            if (assignment.dedicated.isValid()) {
                (void)releaseAllocation(
                    assignment.dedicated.allocationId,
                    assignment.dedicated.reservationToken);
            }
            if (!assignment.p2pGrants.empty() && _impl->p2pSessions) {
                const auto host = std::find_if(
                    assignment.p2pGrants.begin(), assignment.p2pGrants.end(),
                    [](const P2PSessionGrant& grant) {
                        return grant.member.peerId == grant.session.hostPeerId;
                    });
                if (host != assignment.p2pGrants.end()) {
                    (void)_impl->p2pSessions->leaveSession({host->member});
                }
            }
            _impl->setDatabaseError("failed to finalize durable match batch");
            break;
        }
        ++processed;
    }
    return processed;
}

} // namespace ayt::net
