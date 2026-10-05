#pragma once

#include <cstdint>
#include <unordered_set>
#include <unordered_map>

#include <base/types.h>
#include <Core/QualifiedTableName.h>

namespace DB
{

namespace ErrorCodes
{
}

class ReadBuffer;
class WriteBuffer;


/// The following are request-response messages for TablesStatus request of the client-server protocol.
/// Client can ask for about a set of tables and the server will respond with the following information for each table:
/// - Is the table Replicated?
/// - If yes, replication delay for that table.
///
/// For nonexistent tables there will be no TableStatus entry in the response.

struct TableStatus
{
    bool is_replicated = false;
    UInt32 absolute_delay = 0;
    /// Used to filter such nodes out for INSERTs
    bool is_readonly = false;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

/// How many tables an interserver peer may ask about. Its request body is deserialized before the
/// peer has proven knowledge of the cluster secret - on the signed path by construction, because the
/// authentication hash covers the body, and on the unsigned path whenever the request is not
/// rejected outright (`interserver_tables_status_require_auth`) - so the count cannot be left at the
/// generic array bound. `ConnectionEstablisher` is the only producer of a `TablesStatusRequest` and
/// it asks about exactly one table, the remote table behind the `Distributed` table being read, so
/// this leaves ample headroom, including for the "request status for joined tables also" TODO there.
///
/// The names themselves need no extra bound: they are read with `readStringBinaryGrowing`, so a size
/// a peer declares never becomes an allocation unless the peer actually sends the bytes, and what it
/// does send is bounded as every other pre-authentication string in the protocol is. Bounding the
/// name length instead would mean inventing a ceiling that ClickHouse does not otherwise have -
/// `IDatabase::checkTableNameLength` is a no-op unless the database is a `DatabaseOnDisk` - and the
/// name here comes from the `Distributed` engine arguments, so such a ceiling would be able to
/// reject a legitimate request.
static constexpr size_t MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST = 64;

/// Who sent the request, which is what the bound above follows from. A source rather than the limit
/// itself, so that a call site cannot hand an interserver connection the generic bound by mistake -
/// the mapping lives in one place, `TablesStatusRequest::read`.
enum class TablesStatusRequestSource : uint8_t
{
    /// An authenticated client: the generic array bound, as before this bound existed.
    Client,
    /// An interserver peer, bounded by `MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST`.
    InterserverPeer,
};

struct TablesStatusRequest
{
    std::unordered_set<QualifiedTableName> tables;

    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 client_protocol_revision, TablesStatusRequestSource source);

    /// Deterministic, order-independent digest of `tables` for the interserver auth hash.
    std::string getAuthDigest() const;
};

struct TablesStatusResponse
{
    std::unordered_map<QualifiedTableName, TableStatus> table_states_by_id;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

}
