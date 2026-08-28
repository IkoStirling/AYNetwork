#include <AYNetwork/Session/SqliteSessionService.h>

#include <AYCrypto.h>

#include <sodium.h>
#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

constexpr char kHex[] = "0123456789abcdef";
constexpr size_t kTokenBytes = 32;
constexpr size_t kSealedTokenBytes =
    crypto_aead_chacha20poly1305_ietf_NPUBBYTES + kTokenBytes +
    crypto_aead_chacha20poly1305_ietf_ABYTES;
constexpr std::array<uint8_t, kTokenBytes> kStorageCheck = {
    'A','Y','N','e','t','w','o','r','k','-','S','e','s','s','i','o',
    'n','-','S','t','o','r','a','g','e','-','K','e','y','-','V','1'
};

uint64_t systemUnixSeconds() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

bool hasNonZero(const std::array<uint8_t, 32>& value) {
    return std::any_of(value.begin(), value.end(),
                       [](uint8_t byte) { return byte != 0; });
}

std::string toHex(const uint8_t* bytes, size_t size) {
    std::string out(size * 2, '0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = kHex[bytes[i] >> 4];
        out[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    return out;
}

int hexValue(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

bool fromHex(const std::string& text, uint8_t* bytes, size_t size) {
    if (!bytes || text.size() != size * 2) return false;
    for (size_t i = 0; i < size; ++i) {
        const int hi = hexValue(text[i * 2]);
        const int lo = hexValue(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        bytes[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

bool sealToken(const std::array<uint8_t, 32>& key,
               const std::array<uint8_t, kTokenBytes>& plain,
               std::vector<uint8_t>& sealed) {
    sealed.assign(kSealedTokenBytes, 0);
    uint8_t* nonce = sealed.data();
    uint8_t* cipher = nonce + crypto_aead_chacha20poly1305_ietf_NPUBBYTES;
    randombytes_buf(nonce, crypto_aead_chacha20poly1305_ietf_NPUBBYTES);
    unsigned long long cipherSize = 0;
    if (crypto_aead_chacha20poly1305_ietf_encrypt(
            cipher, &cipherSize, plain.data(), plain.size(), nullptr, 0,
            nullptr, nonce, key.data()) != 0 ||
        cipherSize != plain.size() + crypto_aead_chacha20poly1305_ietf_ABYTES) {
        sealed.clear();
        return false;
    }
    return true;
}

bool openToken(const std::array<uint8_t, 32>& key,
               const void* bytes, size_t size,
               std::array<uint8_t, kTokenBytes>& plain) {
    plain = {};
    if (!bytes || size != kSealedTokenBytes) return false;
    const auto* sealed = static_cast<const uint8_t*>(bytes);
    const uint8_t* nonce = sealed;
    const uint8_t* cipher =
        sealed + crypto_aead_chacha20poly1305_ietf_NPUBBYTES;
    const size_t cipherSize =
        size - crypto_aead_chacha20poly1305_ietf_NPUBBYTES;
    unsigned long long plainSize = 0;
    if (crypto_aead_chacha20poly1305_ietf_decrypt(
            plain.data(), &plainSize, nullptr, cipher, cipherSize,
            nullptr, 0, nonce, key.data()) != 0 ||
        plainSize != plain.size()) {
        plain = {};
        return false;
    }
    return true;
}

class Statement {
public:
    Statement(sqlite3* db, const char* sql) {
        if (db && sqlite3_prepare_v2(db, sql, -1, &_stmt, nullptr) != SQLITE_OK) {
            _stmt = nullptr;
        }
    }
    ~Statement() { if (_stmt) sqlite3_finalize(_stmt); }
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    explicit operator bool() const { return _stmt != nullptr; }
    sqlite3_stmt* get() const { return _stmt; }

private:
    sqlite3_stmt* _stmt = nullptr;
};

bool bindU64(sqlite3_stmt* stmt, int index, uint64_t value) {
    if (value > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)())) {
        return false;
    }
    return sqlite3_bind_int64(stmt, index, static_cast<sqlite3_int64>(value)) ==
           SQLITE_OK;
}

bool bindText(sqlite3_stmt* stmt, int index, const std::string& value) {
    return sqlite3_bind_text(stmt, index, value.data(),
                             static_cast<int>(value.size()), SQLITE_TRANSIENT) ==
           SQLITE_OK;
}

bool bindBlob(sqlite3_stmt* stmt, int index, const std::vector<uint8_t>& value) {
    return sqlite3_bind_blob(stmt, index, value.data(),
                             static_cast<int>(value.size()), SQLITE_TRANSIENT) ==
           SQLITE_OK;
}

std::vector<uint8_t> columnBlob(sqlite3_stmt* stmt, int column) {
    const auto* bytes = static_cast<const uint8_t*>(
        sqlite3_column_blob(stmt, column));
    const int size = sqlite3_column_bytes(stmt, column);
    if (!bytes || size <= 0) return {};
    return std::vector<uint8_t>(bytes, bytes + size);
}

std::string columnText(sqlite3_stmt* stmt, int column) {
    const auto* text = sqlite3_column_text(stmt, column);
    const int size = sqlite3_column_bytes(stmt, column);
    if (!text || size <= 0) return {};
    return std::string(reinterpret_cast<const char*>(text),
                       static_cast<size_t>(size));
}

} // namespace

bool SqliteP2PSessionServiceConfig::isValid() const {
    return !databasePath.empty() && !publicSignalingAddress.empty() &&
           signalingPort != 0 && hostLeaseSeconds != 0 &&
           joinTicketLifetimeSeconds != 0 &&
           signalingTokenLifetimeSeconds != 0 && busyTimeoutMs != 0 &&
           maxSessions != 0 && hasNonZero(storageKey);
}

struct SqliteP2PSessionService::Impl {
    struct Session {
        uint64_t id = 0;
        uint32_t epoch = 0;
        PeerId host;
        uint16_t virtualPort = 0;
        uint16_t capacity = 0;
        bool closed = false;
        uint64_t leaseExpiresAt = 0;
        std::string room;
        uint16_t memberCount = 0;
    };

    struct Member {
        PeerId peer;
        std::vector<uint8_t> memberToken;
        std::vector<uint8_t> signalingToken;
        uint64_t signalingExpiresAt = 0;
    };

    Impl(SqliteP2PSessionServiceConfig input,
         const SessionTicketKeyPair& inputKeys)
        : config(std::move(input)), keys(inputKeys) {
        if (!config.nowUnixSeconds) config.nowUnixSeconds = systemUnixSeconds;
        if (!config.isValid() || !validateSessionTicketKeyPair(keys) ||
            sodium_init() < 0) {
            lastError = "invalid durable session configuration";
            return;
        }
        if (sqlite3_open_v2(
                config.databasePath.c_str(), &db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                    SQLITE_OPEN_FULLMUTEX,
                nullptr) != SQLITE_OK) {
            setDatabaseError("failed to open session database");
            close();
            return;
        }
        sqlite3_busy_timeout(db, static_cast<int>(config.busyTimeoutMs));
        if (!exec("PRAGMA foreign_keys=ON") ||
            !exec("PRAGMA journal_mode=WAL") ||
            !exec("PRAGMA synchronous=FULL") ||
            !exec("CREATE TABLE IF NOT EXISTS ay_session_meta("
                  "key TEXT PRIMARY KEY,value BLOB NOT NULL)") ||
            !exec("CREATE TABLE IF NOT EXISTS ay_sessions("
                  "session_id INTEGER PRIMARY KEY,epoch INTEGER NOT NULL,"
                  "host_peer TEXT NOT NULL,virtual_port INTEGER NOT NULL,"
                  "capacity INTEGER NOT NULL,closed INTEGER NOT NULL DEFAULT 0,"
                  "lease_expires INTEGER NOT NULL,room TEXT NOT NULL UNIQUE)") ||
            !exec("CREATE TABLE IF NOT EXISTS ay_session_members("
                  "session_id INTEGER NOT NULL,peer_id TEXT NOT NULL,"
                  "member_token BLOB NOT NULL,signaling_token BLOB NOT NULL,"
                  "signaling_expires INTEGER NOT NULL,"
                  "PRIMARY KEY(session_id,peer_id),"
                  "FOREIGN KEY(session_id) REFERENCES ay_sessions(session_id) "
                  "ON DELETE CASCADE)") ||
            !initializeMetadata()) {
            close();
            return;
        }
        ready = true;
    }

    ~Impl() {
        close();
        ayt::crypto::secureZero(keys.secretKey.data(), keys.secretKey.size());
        ayt::crypto::secureZero(config.storageKey.data(),
                                config.storageKey.size());
    }

    void close() {
        if (db) sqlite3_close(db);
        db = nullptr;
        ready = false;
    }

    uint64_t now() const { return config.nowUnixSeconds(); }

    void setDatabaseError(const char* context) {
        lastError = context;
        if (db) {
            lastError += ": ";
            lastError += sqlite3_errmsg(db);
        }
    }

    bool exec(const char* sql) {
        char* error = nullptr;
        const int result = sqlite3_exec(db, sql, nullptr, nullptr, &error);
        if (result == SQLITE_OK) return true;
        lastError = error ? error : "SQLite operation failed";
        if (error) sqlite3_free(error);
        return false;
    }

    bool begin() { return exec("BEGIN IMMEDIATE"); }
    bool commit() { return exec("COMMIT"); }
    void rollback() { (void)exec("ROLLBACK"); }

    bool readMeta(const char* key, std::vector<uint8_t>& value) {
        Statement stmt(db,
            "SELECT value FROM ay_session_meta WHERE key=?1");
        if (!stmt || sqlite3_bind_text(stmt.get(), 1, key, -1,
                                       SQLITE_STATIC) != SQLITE_OK) return false;
        const int step = sqlite3_step(stmt.get());
        if (step == SQLITE_DONE) {
            value.clear();
            return true;
        }
        if (step != SQLITE_ROW) return false;
        value = columnBlob(stmt.get(), 0);
        return !value.empty();
    }

    bool writeMeta(const char* key, const void* value, size_t size) {
        Statement stmt(db,
            "INSERT INTO ay_session_meta(key,value) VALUES(?1,?2)");
        return stmt &&
               sqlite3_bind_text(stmt.get(), 1, key, -1, SQLITE_STATIC) ==
                   SQLITE_OK &&
               sqlite3_bind_blob(stmt.get(), 2, value,
                   static_cast<int>(size), SQLITE_TRANSIENT) == SQLITE_OK &&
               sqlite3_step(stmt.get()) == SQLITE_DONE;
    }

    bool initializeMetadata() {
        if (!begin()) return false;
        std::vector<uint8_t> publicKey;
        std::vector<uint8_t> storageCheck;
        if (!readMeta("ticket_public_key", publicKey) ||
            !readMeta("storage_key_check", storageCheck)) {
            rollback();
            setDatabaseError("failed to read session metadata");
            return false;
        }
        if (publicKey.empty()) {
            std::vector<uint8_t> sealedCheck;
            if (!sealToken(config.storageKey, kStorageCheck, sealedCheck) ||
                !writeMeta("ticket_public_key", keys.publicKey.data(),
                           keys.publicKey.size()) ||
                !writeMeta("storage_key_check", sealedCheck.data(),
                           sealedCheck.size()) || !commit()) {
                rollback();
                setDatabaseError("failed to initialize session metadata");
                return false;
            }
            return true;
        }
        std::array<uint8_t, kTokenBytes> openedCheck{};
        const bool valid =
            publicKey.size() == keys.publicKey.size() &&
            sodium_memcmp(publicKey.data(), keys.publicKey.data(),
                          keys.publicKey.size()) == 0 &&
            openToken(config.storageKey, storageCheck.data(),
                      storageCheck.size(), openedCheck) &&
            sodium_memcmp(openedCheck.data(), kStorageCheck.data(),
                          openedCheck.size()) == 0;
        ayt::crypto::secureZero(openedCheck.data(), openedCheck.size());
        if (!valid) {
            rollback();
            lastError = "session database key mismatch";
            return false;
        }
        return commit();
    }

    bool loadSession(uint64_t id, Session& session) {
        Statement stmt(db,
            "SELECT s.session_id,s.epoch,s.host_peer,s.virtual_port,"
            "s.capacity,s.closed,s.lease_expires,s.room,COUNT(m.peer_id) "
            "FROM ay_sessions s LEFT JOIN ay_session_members m "
            "ON m.session_id=s.session_id WHERE s.session_id=?1 "
            "GROUP BY s.session_id");
        if (!stmt || !bindU64(stmt.get(), 1, id)) return false;
        if (sqlite3_step(stmt.get()) != SQLITE_ROW) return false;
        session.id = static_cast<uint64_t>(sqlite3_column_int64(stmt.get(), 0));
        session.epoch = static_cast<uint32_t>(sqlite3_column_int64(stmt.get(), 1));
        session.host = PeerId{columnText(stmt.get(), 2)};
        session.virtualPort = static_cast<uint16_t>(
            sqlite3_column_int(stmt.get(), 3));
        session.capacity = static_cast<uint16_t>(
            sqlite3_column_int(stmt.get(), 4));
        session.closed = sqlite3_column_int(stmt.get(), 5) != 0;
        session.leaseExpiresAt = static_cast<uint64_t>(
            sqlite3_column_int64(stmt.get(), 6));
        session.room = columnText(stmt.get(), 7);
        session.memberCount = static_cast<uint16_t>(
            sqlite3_column_int(stmt.get(), 8));
        return session.id != 0 && session.epoch != 0 &&
               session.host.isValid() && session.virtualPort != 0 &&
               session.capacity != 0 && session.memberCount != 0 &&
               !session.room.empty();
    }

    bool loadMember(uint64_t sessionId, const PeerId& peer, Member& member) {
        Statement stmt(db,
            "SELECT peer_id,member_token,signaling_token,signaling_expires "
            "FROM ay_session_members WHERE session_id=?1 AND peer_id=?2");
        if (!stmt || !bindU64(stmt.get(), 1, sessionId) ||
            !bindText(stmt.get(), 2, peer.value) ||
            sqlite3_step(stmt.get()) != SQLITE_ROW) return false;
        member.peer = PeerId{columnText(stmt.get(), 0)};
        member.memberToken = columnBlob(stmt.get(), 1);
        member.signalingToken = columnBlob(stmt.get(), 2);
        member.signalingExpiresAt = static_cast<uint64_t>(
            sqlite3_column_int64(stmt.get(), 3));
        return member.peer.isValid() &&
               member.memberToken.size() == kSealedTokenBytes &&
               member.signalingToken.size() == kSealedTokenBytes;
    }

    bool authenticate(const P2PSessionMemberCredential& credential,
                      Member& member) {
        if (!credential.isValid() ||
            !loadMember(credential.sessionId, credential.peerId, member)) {
            return false;
        }
        std::array<uint8_t, kTokenBytes> expected{};
        std::array<uint8_t, kTokenBytes> supplied{};
        const bool decoded = openToken(
            config.storageKey, member.memberToken.data(),
            member.memberToken.size(), expected) &&
            fromHex(credential.token, supplied.data(), supplied.size());
        const bool equal = decoded &&
            sodium_memcmp(expected.data(), supplied.data(), expected.size()) == 0;
        ayt::crypto::secureZero(expected.data(), expected.size());
        ayt::crypto::secureZero(supplied.data(), supplied.size());
        return equal;
    }

    P2PBackendSessionInfo describe(const Session& session) const {
        P2PBackendSessionInfo info;
        info.sessionId = session.id;
        info.epoch = session.epoch;
        info.hostPeerId = session.host;
        info.virtualPort = session.virtualPort;
        info.capacity = session.capacity;
        info.memberCount = session.memberCount;
        info.open = !session.closed && now() < session.leaseExpiresAt &&
                    session.memberCount < session.capacity;
        info.hostLeaseExpiresAtUnixSeconds = session.leaseExpiresAt;
        info.signalingAddress = config.publicSignalingAddress;
        info.signalingPort = config.signalingPort;
        info.signalingRoom = session.room;
        return info;
    }

    SessionServiceResult<P2PSessionGrant> makeGrant(
        const Session& session, const PeerId& peer,
        const std::array<uint8_t, kTokenBytes>& memberToken,
        const std::array<uint8_t, kTokenBytes>& signalingToken,
        uint64_t nowSeconds) const {
        SessionJoinTicketClaims claims;
        claims.sessionId = session.id;
        claims.epoch = session.epoch;
        claims.peerId = peer;
        claims.issuedAtUnixSeconds = nowSeconds;
        claims.expiresAtUnixSeconds =
            nowSeconds + config.joinTicketLifetimeSeconds;
        ayt::crypto::generateRandomBytes(claims.nonce.data(), claims.nonce.size());

        P2PSessionGrant grant;
        grant.session = describe(session);
        grant.member.sessionId = session.id;
        grant.member.peerId = peer;
        grant.member.token = toHex(memberToken.data(), memberToken.size());
        grant.signalingToken = toHex(
            signalingToken.data(), signalingToken.size());
        grant.ticketPublicKey = keys.publicKey;
        if (!issueSessionJoinTicket(claims, keys.secretKey, grant.joinTicket)) {
            return SessionServiceResult<P2PSessionGrant>::failure(
                SessionServiceError::InternalError,
                "failed to issue durable Join Ticket");
        }
        return SessionServiceResult<P2PSessionGrant>::success(std::move(grant));
    }

    SessionServiceResult<P2PSessionGrant> create(
        const P2PSessionCreateRequest& request) {
        if (!ready || !request.hostPeerId.isValid() || request.virtualPort == 0 ||
            request.capacity == 0 || request.capacity > kP2PMaxSessionMembers) {
            return SessionServiceResult<P2PSessionGrant>::failure(
                SessionServiceError::InvalidRequest,
                "invalid durable create-session request");
        }
        if (!begin()) return databaseFailure<P2PSessionGrant>();
        Statement countStmt(db, "SELECT COUNT(*) FROM ay_sessions");
        if (!countStmt || sqlite3_step(countStmt.get()) != SQLITE_ROW ||
            static_cast<uint64_t>(sqlite3_column_int64(countStmt.get(), 0)) >=
                config.maxSessions) {
            rollback();
            return SessionServiceResult<P2PSessionGrant>::failure(
                SessionServiceError::SessionFull,
                "durable session directory is full");
        }

        Session session;
        for (unsigned attempt = 0; attempt < 32 && session.id == 0; ++attempt) {
            uint64_t candidate = 0;
            ayt::crypto::generateRandomBytes(
                reinterpret_cast<uint8_t*>(&candidate), sizeof(candidate));
            candidate &= static_cast<uint64_t>(
                (std::numeric_limits<int64_t>::max)());
            if (candidate == 0) continue;
            Statement exists(db,
                "SELECT 1 FROM ay_sessions WHERE session_id=?1");
            if (!exists || !bindU64(exists.get(), 1, candidate)) continue;
            if (sqlite3_step(exists.get()) == SQLITE_DONE) session.id = candidate;
        }
        if (session.id == 0) {
            rollback();
            return SessionServiceResult<P2PSessionGrant>::failure(
                SessionServiceError::InternalError,
                "failed to allocate durable session id");
        }
        const uint64_t nowSeconds = now();
        session.epoch = 1;
        session.host = request.hostPeerId;
        session.virtualPort = request.virtualPort;
        session.capacity = request.capacity;
        session.leaseExpiresAt = nowSeconds + config.hostLeaseSeconds;
        session.room = "s-" + toHex(
            reinterpret_cast<const uint8_t*>(&session.id), sizeof(session.id));
        session.memberCount = 1;

        std::array<uint8_t, kTokenBytes> memberToken{};
        std::array<uint8_t, kTokenBytes> signalingToken{};
        ayt::crypto::generateRandomBytes(memberToken.data(), memberToken.size());
        ayt::crypto::generateRandomBytes(
            signalingToken.data(), signalingToken.size());
        std::vector<uint8_t> sealedMember;
        std::vector<uint8_t> sealedSignaling;
        if (!hasNonZero(memberToken) || !hasNonZero(signalingToken) ||
            !sealToken(config.storageKey, memberToken, sealedMember) ||
            !sealToken(config.storageKey, signalingToken, sealedSignaling)) {
            rollback();
            return SessionServiceResult<P2PSessionGrant>::failure(
                SessionServiceError::InternalError,
                "failed to encrypt durable member credentials");
        }

        Statement insertSession(db,
            "INSERT INTO ay_sessions(session_id,epoch,host_peer,virtual_port,"
            "capacity,closed,lease_expires,room) "
            "VALUES(?1,1,?2,?3,?4,0,?5,?6)");
        Statement insertMember(db,
            "INSERT INTO ay_session_members(session_id,peer_id,member_token,"
            "signaling_token,signaling_expires) VALUES(?1,?2,?3,?4,?5)");
        const uint64_t signalingExpiry =
            nowSeconds + config.signalingTokenLifetimeSeconds;
        const bool inserted = insertSession && insertMember &&
            bindU64(insertSession.get(), 1, session.id) &&
            bindText(insertSession.get(), 2, session.host.value) &&
            sqlite3_bind_int(insertSession.get(), 3, session.virtualPort) == SQLITE_OK &&
            sqlite3_bind_int(insertSession.get(), 4, session.capacity) == SQLITE_OK &&
            bindU64(insertSession.get(), 5, session.leaseExpiresAt) &&
            bindText(insertSession.get(), 6, session.room) &&
            sqlite3_step(insertSession.get()) == SQLITE_DONE &&
            bindU64(insertMember.get(), 1, session.id) &&
            bindText(insertMember.get(), 2, session.host.value) &&
            bindBlob(insertMember.get(), 3, sealedMember) &&
            bindBlob(insertMember.get(), 4, sealedSignaling) &&
            bindU64(insertMember.get(), 5, signalingExpiry) &&
            sqlite3_step(insertMember.get()) == SQLITE_DONE;
        auto grant = makeGrant(session, session.host, memberToken,
                               signalingToken, nowSeconds);
        ayt::crypto::secureZero(memberToken.data(), memberToken.size());
        ayt::crypto::secureZero(signalingToken.data(), signalingToken.size());
        if (!inserted || !grant || !commit()) {
            rollback();
            return inserted && !grant ? grant : databaseFailure<P2PSessionGrant>();
        }
        return grant;
    }

    template <typename T>
    SessionServiceResult<T> databaseFailure() {
        setDatabaseError("session database operation failed");
        return SessionServiceResult<T>::failure(
            SessionServiceError::InternalError, lastError);
    }

    SqliteP2PSessionServiceConfig config;
    SessionTicketKeyPair keys{};
    sqlite3* db = nullptr;
    mutable std::mutex mutex;
    bool ready = false;
    std::string lastError;
};

SqliteP2PSessionService::SqliteP2PSessionService(
    SqliteP2PSessionServiceConfig config,
    const SessionTicketKeyPair& ticketKeys)
    : _impl(std::make_unique<Impl>(std::move(config), ticketKeys)) {}

SqliteP2PSessionService::~SqliteP2PSessionService() = default;

bool SqliteP2PSessionService::isReady() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->ready;
}

std::string SqliteP2PSessionService::getLastError() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->lastError;
}

SessionTicketPublicKey SqliteP2PSessionService::getTicketPublicKey() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->keys.publicKey;
}

SessionServiceResult<P2PSessionGrant>
SqliteP2PSessionService::createSession(
    const P2PSessionCreateRequest& request) {
    std::lock_guard lock(_impl->mutex);
    return _impl->create(request);
}

SessionServiceResult<P2PSessionGrant>
SqliteP2PSessionService::joinSession(
    const P2PSessionJoinRequest& request) {
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || request.sessionId == 0 || !request.peerId.isValid()) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InvalidRequest, "invalid durable join request");
    }
    if (!_impl->begin()) return _impl->databaseFailure<P2PSessionGrant>();
    Impl::Session session;
    if (!_impl->loadSession(request.sessionId, session)) {
        _impl->rollback();
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    const uint64_t now = _impl->now();
    if (session.closed) {
        _impl->rollback();
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionClosed, "session is closed");
    }
    if (now >= session.leaseExpiresAt) {
        _impl->rollback();
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::HostLeaseExpired, "Host lease expired");
    }
    Impl::Member existing;
    if (_impl->loadMember(session.id, request.peerId, existing)) {
        _impl->rollback();
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::Unauthorized, "peer is already a member");
    }
    if (session.memberCount >= session.capacity) {
        _impl->rollback();
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionFull, "session is full");
    }

    std::array<uint8_t, kTokenBytes> memberToken{};
    std::array<uint8_t, kTokenBytes> signalingToken{};
    ayt::crypto::generateRandomBytes(memberToken.data(), memberToken.size());
    ayt::crypto::generateRandomBytes(signalingToken.data(), signalingToken.size());
    std::vector<uint8_t> sealedMember;
    std::vector<uint8_t> sealedSignaling;
    if (!sealToken(_impl->config.storageKey, memberToken, sealedMember) ||
        !sealToken(_impl->config.storageKey, signalingToken, sealedSignaling)) {
        _impl->rollback();
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InternalError,
            "failed to encrypt durable member credentials");
    }
    Statement insert(_impl->db,
        "INSERT INTO ay_session_members(session_id,peer_id,member_token,"
        "signaling_token,signaling_expires) VALUES(?1,?2,?3,?4,?5)");
    const bool inserted = insert && bindU64(insert.get(), 1, session.id) &&
        bindText(insert.get(), 2, request.peerId.value) &&
        bindBlob(insert.get(), 3, sealedMember) &&
        bindBlob(insert.get(), 4, sealedSignaling) &&
        bindU64(insert.get(), 5,
                now + _impl->config.signalingTokenLifetimeSeconds) &&
        sqlite3_step(insert.get()) == SQLITE_DONE;
    ++session.memberCount;
    auto grant = _impl->makeGrant(
        session, request.peerId, memberToken, signalingToken, now);
    ayt::crypto::secureZero(memberToken.data(), memberToken.size());
    ayt::crypto::secureZero(signalingToken.data(), signalingToken.size());
    if (!inserted || !grant || !_impl->commit()) {
        _impl->rollback();
        return inserted && !grant ? grant
                                  : _impl->databaseFailure<P2PSessionGrant>();
    }
    return grant;
}

SessionServiceResult<P2PBackendSessionInfo>
SqliteP2PSessionService::heartbeat(
    const P2PSessionHeartbeatRequest& request) {
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin()) {
        return _impl->databaseFailure<P2PBackendSessionInfo>();
    }
    Impl::Session session;
    if (!_impl->loadSession(request.member.sessionId, session)) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Member member;
    if (!_impl->authenticate(request.member, member) ||
        member.peer != session.host) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::Unauthorized,
            "only the current Host may heartbeat");
    }
    if (request.expectedEpoch != session.epoch) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::EpochConflict, "authority epoch changed");
    }
    const uint64_t now = _impl->now();
    if (now >= session.leaseExpiresAt) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::HostLeaseExpired, "Host lease expired");
    }
    session.leaseExpiresAt = now + _impl->config.hostLeaseSeconds;
    Statement update(_impl->db,
        "UPDATE ay_sessions SET lease_expires=?1 WHERE session_id=?2 "
        "AND epoch=?3 AND host_peer=?4");
    const bool changed = update &&
        bindU64(update.get(), 1, session.leaseExpiresAt) &&
        bindU64(update.get(), 2, session.id) &&
        sqlite3_bind_int64(update.get(), 3, session.epoch) == SQLITE_OK &&
        bindText(update.get(), 4, session.host.value) &&
        sqlite3_step(update.get()) == SQLITE_DONE &&
        sqlite3_changes(_impl->db) == 1;
    if (!changed || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<P2PBackendSessionInfo>();
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(
        _impl->describe(session));
}

SessionServiceResult<P2PBackendSessionInfo>
SqliteP2PSessionService::claimHost(
    const P2PSessionClaimHostRequest& request) {
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || request.expectedEpoch == 0 ||
        !request.newHostPeerId.isValid()) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest, "invalid Host claim");
    }
    if (!_impl->begin()) return _impl->databaseFailure<P2PBackendSessionInfo>();
    Impl::Session session;
    if (!_impl->loadSession(request.member.sessionId, session)) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Member caller;
    if (!_impl->authenticate(request.member, caller)) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::Unauthorized, "invalid member credential");
    }
    if (request.expectedEpoch != session.epoch) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::EpochConflict, "authority epoch changed");
    }
    Impl::Member target;
    if (request.newHostPeerId == session.host ||
        !_impl->loadMember(session.id, request.newHostPeerId, target)) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest,
            "new Host must be a different admitted member");
    }
    const uint64_t now = _impl->now();
    const bool graceful = now < session.leaseExpiresAt &&
                          caller.peer == session.host;
    const bool expiredSelfClaim = now >= session.leaseExpiresAt &&
                                  caller.peer == request.newHostPeerId;
    if (!graceful && !expiredSelfClaim) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::Unauthorized,
            "active Host lease prevents this claim");
    }
    if (session.epoch == (std::numeric_limits<uint32_t>::max)()) {
        _impl->rollback();
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InternalError, "authority epoch exhausted");
    }
    const uint32_t oldEpoch = session.epoch;
    ++session.epoch;
    session.host = request.newHostPeerId;
    session.leaseExpiresAt = now + _impl->config.hostLeaseSeconds;
    Statement update(_impl->db,
        "UPDATE ay_sessions SET epoch=?1,host_peer=?2,lease_expires=?3 "
        "WHERE session_id=?4 AND epoch=?5");
    const bool changed = update &&
        sqlite3_bind_int64(update.get(), 1, session.epoch) == SQLITE_OK &&
        bindText(update.get(), 2, session.host.value) &&
        bindU64(update.get(), 3, session.leaseExpiresAt) &&
        bindU64(update.get(), 4, session.id) &&
        sqlite3_bind_int64(update.get(), 5, oldEpoch) == SQLITE_OK &&
        sqlite3_step(update.get()) == SQLITE_DONE &&
        sqlite3_changes(_impl->db) == 1;
    if (!changed || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<P2PBackendSessionInfo>();
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(
        _impl->describe(session));
}

SessionServiceResult<SessionServiceEmpty>
SqliteP2PSessionService::leaveSession(
    const P2PSessionLeaveRequest& request) {
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !_impl->begin()) {
        return _impl->databaseFailure<SessionServiceEmpty>();
    }
    Impl::Session session;
    if (!_impl->loadSession(request.member.sessionId, session)) {
        _impl->rollback();
        return SessionServiceResult<SessionServiceEmpty>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Member member;
    if (!_impl->authenticate(request.member, member)) {
        _impl->rollback();
        return SessionServiceResult<SessionServiceEmpty>::failure(
            SessionServiceError::Unauthorized, "invalid member credential");
    }
    const bool leavingHost = member.peer == session.host;
    if (leavingHost && _impl->now() >= session.leaseExpiresAt) {
        _impl->rollback();
        return SessionServiceResult<SessionServiceEmpty>::failure(
            SessionServiceError::HostLeaseExpired,
            "expired Host cannot terminate the session");
    }
    Statement remove(_impl->db, leavingHost
        ? "DELETE FROM ay_sessions WHERE session_id=?1"
        : "DELETE FROM ay_session_members WHERE session_id=?1 AND peer_id=?2");
    bool changed = remove && bindU64(remove.get(), 1, session.id);
    if (!leavingHost) changed = changed &&
        bindText(remove.get(), 2, member.peer.value);
    changed = changed && sqlite3_step(remove.get()) == SQLITE_DONE &&
              sqlite3_changes(_impl->db) == 1;
    if (!changed || !_impl->commit()) {
        _impl->rollback();
        return _impl->databaseFailure<SessionServiceEmpty>();
    }
    return SessionServiceResult<SessionServiceEmpty>::success({});
}

SessionServiceResult<P2PBackendSessionInfo>
SqliteP2PSessionService::getSession(uint64_t sessionId) {
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || sessionId == 0) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest, "invalid session id");
    }
    Impl::Session session;
    if (!_impl->loadSession(sessionId, session)) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(
        _impl->describe(session));
}

bool SqliteP2PSessionService::resolveSignalingCredential(
    const PeerId& peerId, const std::string& room,
    std::array<uint8_t, 32>& token,
    uint64_t& expiresAtUnixSeconds) const {
    token = {};
    expiresAtUnixSeconds = 0;
    std::lock_guard lock(_impl->mutex);
    if (!_impl->ready || !peerId.isValid() || room.empty()) return false;
    Statement stmt(_impl->db,
        "SELECT m.signaling_token,m.signaling_expires,s.closed "
        "FROM ay_session_members m JOIN ay_sessions s "
        "ON s.session_id=m.session_id WHERE s.room=?1 AND m.peer_id=?2");
    if (!stmt || !bindText(stmt.get(), 1, room) ||
        !bindText(stmt.get(), 2, peerId.value) ||
        sqlite3_step(stmt.get()) != SQLITE_ROW ||
        sqlite3_column_int(stmt.get(), 2) != 0) return false;
    const auto sealed = columnBlob(stmt.get(), 0);
    const uint64_t expiry = static_cast<uint64_t>(
        sqlite3_column_int64(stmt.get(), 1));
    if (expiry <= _impl->now() ||
        !openToken(_impl->config.storageKey, sealed.data(),
                   sealed.size(), token)) {
        token = {};
        return false;
    }
    expiresAtUnixSeconds = expiry;
    return true;
}

} // namespace ayt::net
