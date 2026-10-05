#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TablesStatus.h>
#include <Common/Exception.h>

#include <optional>
#include <string>

namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_LARGE_STRING_SIZE;
}

using namespace DB;

namespace
{

/// A `TablesStatusRequest` body as a hostile peer could put it on the wire. Built by hand rather
/// than with `TablesStatusRequest::write`, so that a table name can declare a length that is never
/// followed by its bytes - which is the case the name bound exists for.
std::string requestBody(size_t table_count, std::optional<size_t> declared_name_size = {})
{
    WriteBufferFromOwnString out;
    writeVarUInt(table_count, out);
    for (size_t i = 0; i < table_count; ++i)
    {
        writeStringBinary(std::string("default"), out);
        if (declared_name_size.has_value())
            writeVarUInt(*declared_name_size, out);
        else
            writeStringBinary("t" + std::to_string(i), out);
    }
    out.finalize();
    return out.str();
}

TablesStatusRequest readBody(const std::string & body, TablesStatusRequestSource source)
{
    ReadBufferFromString in(body);
    TablesStatusRequest request;
    request.read(in, DBMS_MIN_REVISION_WITH_TABLES_STATUS, source);
    return request;
}

}

/// An interserver request is deserialized before the peer has proven knowledge of the cluster
/// secret, so `read` bounds it by `INTERSERVER_TABLES_STATUS_REQUEST_LIMITS`. At the cap it still
/// parses - the bound must not reject what a legitimate peer could send.
TEST(TablesStatusRequestRead, AcceptsTheInterserverTableCap)
{
    const size_t cap = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_tables;
    auto request = readBody(requestBody(cap), TablesStatusRequestSource::InterserverPeer);
    EXPECT_EQ(request.tables.size(), cap);
}

TEST(TablesStatusRequestRead, RejectsOneTableOverTheInterserverCap)
{
    const size_t over_cap = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_tables + 1;
    /// The count is checked before any name is read, so the body does not have to be complete.
    try
    {
        readBody(requestBody(over_cap), TablesStatusRequestSource::InterserverPeer);
        FAIL() << "a request over the interserver table cap was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_ARRAY_SIZE);
    }
}

/// The table count alone does not bound the request: `readStringBinary` reserves the declared size
/// of a name before reading its bytes. Here the declared length is followed by no bytes at all, so
/// a server honouring the budget refuses on the length, while one that does not would reserve it
/// and only then fail on the truncated buffer. One byte over the budget on purpose - a huge
/// declaration would also be refused by the memory tracker, which would pass without any budget.
TEST(TablesStatusRequestRead, RejectsNamesOverTheInterserverBudget)
{
    const size_t over_budget = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_total_name_size + 1;
    try
    {
        readBody(requestBody(1, over_budget), TablesStatusRequestSource::InterserverPeer);
        FAIL() << "a table name declared over the interserver name budget was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

/// The budget is shared across the request rather than applied to each name, so that it bounds the
/// memory without capping how long an individual name may be. ClickHouse has no such cap to borrow:
/// `IDatabase::checkTableNameLength` is a no-op unless the database is a `DatabaseOnDisk`. A single
/// name that uses up the whole budget must therefore still parse.
TEST(TablesStatusRequestRead, AcceptsOneNameUpToTheWholeInterserverBudget)
{
    const size_t budget = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_total_name_size;
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<size_t>(1), out);
    writeStringBinary(std::string(), out);                 /// empty database, so the name gets it all
    writeStringBinary(std::string(budget, 'x'), out);
    out.finalize();

    auto request = readBody(out.str(), TablesStatusRequestSource::InterserverPeer);
    ASSERT_EQ(request.tables.size(), 1u);
    EXPECT_EQ(request.tables.begin()->table.size(), budget);
}

/// ... and because it is shared, names that each fit but together do not must be refused. This is
/// what stops `max_tables` names of the maximum length from multiplying the bound.
TEST(TablesStatusRequestRead, RejectsNamesThatOnlyExceedTheBudgetTogether)
{
    const size_t budget = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_total_name_size;
    const size_t half = budget / 2;

    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<size_t>(2), out);
    writeStringBinary(std::string(), out);
    writeStringBinary(std::string(half, 'a'), out);
    writeStringBinary(std::string(), out);
    /// Fits the budget on its own, but not what the first name left.
    writeVarUInt(budget - half + 1, out);
    out.finalize();

    try
    {
        readBody(out.str(), TablesStatusRequestSource::InterserverPeer);
        FAIL() << "two names exceeding the interserver budget together were accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

/// The bound follows from the source, so that no call site can hand an interserver connection the
/// generous profile. This pins the other half of that mapping: as `Client`, the very request the
/// interserver cap rejects must still parse, both in table count and in name length.
TEST(TablesStatusRequestRead, ClientIsNotBoundedByTheInterserverCap)
{
    const size_t over_table_cap = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_tables + 1;
    auto many = readBody(requestBody(over_table_cap), TablesStatusRequestSource::Client);
    EXPECT_EQ(many.tables.size(), over_table_cap);

    const size_t over_name_budget = INTERSERVER_TABLES_STATUS_REQUEST_LIMITS.max_total_name_size + 1;
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<size_t>(1), out);
    writeStringBinary(std::string("default"), out);
    writeStringBinary(std::string(over_name_budget, 'x'), out);
    out.finalize();
    auto long_name = readBody(out.str(), TablesStatusRequestSource::Client);
    ASSERT_EQ(long_name.tables.size(), 1u);
    EXPECT_EQ(long_name.tables.begin()->table.size(), over_name_budget);
}
