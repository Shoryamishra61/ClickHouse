#include <Server/IcebergRESTCatalog/IcebergRESTCatalogTableMetadata.h>

#include "config.h"

#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Poco/JSON/Array.h>

#include <chrono>
#include <set>
#include <map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
}

using namespace Iceberg;

namespace
{

/// Partition field ids start here by Iceberg convention. `last-partition-id` of a table without partition fields is one less.
constexpr Int64 PARTITION_FIELD_ID_START = 1000;

Int64 getInteger(const Poco::JSON::Object & object, const String & key, const String & what)
{
    if (!object.has(key) || !object.get(key).isInteger())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have an integer '{}'", what, key);
    return object.getValue<Int64>(key);
}

Poco::JSON::Array::Ptr getArray(const Poco::JSON::Object & object, const String & key, const String & what)
{
    auto array = object.getArray(key);
    if (!array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have an array '{}'", what, key);
    return array;
}

String getString(const Poco::JSON::Object & object, const String & key, const String & what)
{
    if (!object.has(key) || !object.get(key).isString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have a string '{}'", what, key);
    return object.getValue<String>(key);
}

Poco::JSON::Object::Ptr getObject(const Poco::JSON::Object & object, const String & key, const String & what)
{
    auto result = object.getObject(key);
    if (!result)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have an object '{}'", what, key);
    return result;
}

Int64 nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void collectFieldId(const Poco::JSON::Object & holder, const String & key, std::set<Int64> & ids)
{
    const auto id = getInteger(holder, key, "Every schema field");
    if (!ids.insert(id).second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate field id {} in schema", id);
}

void checkRequiredFlag(const Poco::JSON::Object & holder, const String & key, const String & what)
{
    if (!holder.has(key) || !holder.get(key).isBoolean())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have a boolean '{}'", what, key);
}

void collectAndValidateNestedFields(const Poco::Dynamic::Var & type, std::set<Int64> & ids)
{
    if (type.isString())
    {
        /// Reuse the reader's parser so that every persisted primitive type can be read back.
        IcebergSchemaProcessor::getSimpleType(type.extract<String>());
        return;
    }

    if (type.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A schema type must be a string or an object, got '{}'", type.toString());

    const auto & object = type.extract<Poco::JSON::Object::Ptr>();
    const auto kind = object->optValue<String>(f_type, "");
    if (kind == f_struct)
    {
        for (const auto & field : *getArray(*object, f_fields, "A struct type"))
        {
            if (field.type() != typeid(Poco::JSON::Object::Ptr))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must be an object");
            const auto & field_object = field.extract<Poco::JSON::Object::Ptr>();
            collectFieldId(*field_object, f_id, ids);
            if (!field_object->has(f_name) || !field_object->get(f_name).isString() || field_object->getValue<String>(f_name).empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must have a non-empty string 'name'");
            checkRequiredFlag(*field_object, f_required, "Every schema field");
            if (!field_object->has(f_type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every schema field must have a 'type'");
            collectAndValidateNestedFields(field_object->get(f_type), ids);
        }
    }
    else if (kind == f_list)
    {
        collectFieldId(*object, f_element_id, ids);
        checkRequiredFlag(*object, f_element_required, "A list type");
        collectAndValidateNestedFields(object->get(f_element), ids);
    }
    else if (kind == f_map)
    {
        collectFieldId(*object, f_key_id, ids);
        collectAndValidateNestedFields(object->get(f_key), ids);
        collectFieldId(*object, f_value_id, ids);
        checkRequiredFlag(*object, f_value_required, "A map type");
        collectAndValidateNestedFields(object->get(f_value), ids);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown nested type '{}' in schema", kind);
}

void checkString(const Poco::JSON::Object & object, const String & key, const String & what)
{
    if (!object.has(key) || !object.get(key).isString() || object.getValue<String>(key).empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have a non-empty string '{}'", what, key);
}

/// Reuse the reader's parser so that every persisted transform can be read back.
void checkTransform(const String & transform, const String & what)
{
#if USE_AVRO
    if (!parseTransformAndArgument(transform))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform '{}' in {}", transform, what);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cannot validate transform '{}' in {}: ClickHouse was built without Avro support", transform, what);
#endif
}

/// `required_strings` are the mandatory string members of every field besides `source-id`.
std::vector<Poco::JSON::Object::Ptr> getSpecFields(
    const Poco::JSON::Object & spec, const String & spec_name, const std::set<Int64> & schema_ids, const std::vector<String> & required_strings)
{
    const auto what = fmt::format("'{}'", spec_name);
    std::vector<Poco::JSON::Object::Ptr> fields;
    for (const auto & field : *getArray(spec, f_fields, what))
    {
        if (field.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every field of {} must be an object", what);
        const auto & field_object = field.extract<Poco::JSON::Object::Ptr>();

        /// TODO: validate the transform against the source column type, like Java's `PartitionSpec.checkCompatibility`.
        const auto source_id = getInteger(*field_object, f_source_id, what + " field");
        if (!schema_ids.contains(source_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} references unknown source-id {}", what, source_id);
        for (const auto & key : required_strings)
            checkString(*field_object, key, what + " field");
        checkTransform(field_object->getValue<String>(f_transform), what);
        fields.push_back(field_object);
    }
    return fields;
}

/// Assigns `field-id` 1000, 1001, ... to the partition fields in order and returns `last-partition-id`.
/// A `field-id` sent by the client is ignored, like Java's `TableMetadata.newTableMetadata` does.
/// Before the table exists no manifest references these ids, so the server owns them. This also rules out duplicates.
Int64 getLastPartitionId(const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids)
{
    Int64 last_partition_id = PARTITION_FIELD_ID_START - 1;
    for (auto & field : getSpecFields(spec, "partition-spec", schema_ids, {f_name, f_transform}))
        field->set(f_field_id, ++last_partition_id);
    return last_partition_id;
}

/// Returns the `order-id`. The spec reserves 0 for the unsorted order, so a sorted order gets 1 like in Java and pyiceberg.
Int64 getSortOrderId(const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids)
{
    const auto fields = getSpecFields(spec, "write-order", schema_ids, {f_transform, f_direction, f_null_order});
    for (const auto & field : fields)
    {
        const auto direction = field->getValue<String>(f_direction);
        if (direction != "asc" && direction != "desc")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'write-order' direction must be 'asc' or 'desc', got '{}'", direction);
        const auto null_order = field->getValue<String>(f_null_order);
        if (null_order != "nulls-first" && null_order != "nulls-last")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'write-order' null-order must be 'nulls-first' or 'nulls-last', got '{}'", null_order);
    }
    return fields.empty() ? 0 : 1;
}

std::set<Int64> getFieldIds(const Poco::JSON::Object::Ptr & schema)
{
    std::set<Int64> ids;
    collectAndValidateNestedFields(Poco::Dynamic::Var(schema), ids);
    return ids;
}

/// Checks the schema is a struct and marks it as schema 0.
void prepareSchema(Poco::JSON::Object::Ptr schema)
{
    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be an object");
    if (schema->optValue<String>(f_type, f_struct) != f_struct)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be a struct type");
    schema->set(f_type, f_struct);
    schema->set(f_schema_id, 0);
}

/// Replaces a missing spec with an empty one.
Poco::JSON::Object::Ptr prepareSpec(Poco::JSON::Object::Ptr spec)
{
    if (!spec)
    {
        spec = new Poco::JSON::Object;
        spec->set(f_fields, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    }
    return spec;
}

}

/// Normalizes the request objects in place to place them in the metadata.
Poco::JSON::Object::Ptr buildInitialTableMetadata(
    const String & uuid,
    const String & location,
    Poco::JSON::Object::Ptr schema,
    Poco::JSON::Object::Ptr partition_spec,
    Poco::JSON::Object::Ptr write_order,
    std::map<String, String> properties)
{
    /// TODO: support format version 3. The initial file needs `next-row-id: 0`, and the commit path needs v3 handling.
    if (auto it = properties.find(f_format_version); it != properties.end())
    {
        if (it->second != "2")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only format-version 2 is supported, got '{}'", it->second);
        properties.erase(it);
    }

    prepareSchema(schema);
    const auto field_ids = getFieldIds(schema);
    const Int64 last_column_id = field_ids.empty() ? 0 : *field_ids.rbegin();

    partition_spec = prepareSpec(partition_spec);
    partition_spec->set(f_spec_id, 0);
    const auto last_partition_id = getLastPartitionId(*partition_spec, field_ids);

    write_order = prepareSpec(write_order);
    const auto sort_order_id = getSortOrderId(*write_order, field_ids);
    write_order->set(f_order_id, sort_order_id);

    Poco::JSON::Object::Ptr properties_json = new Poco::JSON::Object;
    for (const auto & [key, value] : properties)
        properties_json->set(key, value);

    const auto now_ms = nowMs();

    /// Key order follows the Iceberg spec listing so the file reads like the ones Java writes.
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    metadata->set(f_format_version, 2);
    metadata->set(f_table_uuid, uuid);
    metadata->set(f_location, location);
    metadata->set(f_last_sequence_number, 0);
    metadata->set(f_last_updated_ms, now_ms);
    metadata->set(f_last_column_id, last_column_id);
    metadata->set(f_current_schema_id, 0);
    Poco::JSON::Array::Ptr schemas = new Poco::JSON::Array;
    schemas->add(schema);
    metadata->set(f_schemas, schemas);
    metadata->set(f_default_spec_id, 0);
    Poco::JSON::Array::Ptr partition_specs = new Poco::JSON::Array;
    partition_specs->add(partition_spec);
    metadata->set(f_partition_specs, partition_specs);
    metadata->set(f_last_partition_id, last_partition_id);
    metadata->set(f_default_sort_order_id, sort_order_id);
    Poco::JSON::Array::Ptr sort_orders = new Poco::JSON::Array;
    sort_orders->add(write_order);
    metadata->set(f_sort_orders, sort_orders);
    metadata->set(f_properties, properties_json);
    metadata->set(f_current_snapshot_id, -1);
    /// Java and pyiceberg write an empty `refs` for a table without snapshots.
    metadata->set(f_refs, Poco::JSON::Object::Ptr(new Poco::JSON::Object));
    metadata->set(f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_snapshot_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_metadata_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_partition_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    return metadata;
}


namespace
{

/// `snapshot-id` of `ref`, or nullopt if the ref does not exist.
std::optional<Int64> maybeGetRefSnapshotId(const Poco::JSON::Object & metadata, const String & ref)
{
    const auto refs = metadata.getObject(f_refs);
    if (!refs || !refs->has(ref))
        return std::nullopt;
    const auto ref_object = getObject(*refs, ref, "Ref");
    return getInteger(*ref_object, f_metadata_snapshot_id, "Ref");
}

Poco::JSON::Object::Ptr findSnapshot(const Poco::JSON::Object & metadata, Int64 snapshot_id)
{
    const auto snapshots = getArray(metadata, f_snapshots, "Table metadata");
    for (unsigned i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        if (snapshot && getInteger(*snapshot, f_metadata_snapshot_id, "Snapshot") == snapshot_id)
            return snapshot;
    }
    return nullptr;
}

void addSnapshot(Poco::JSON::Object & metadata, const Poco::JSON::Object::Ptr & new_snapshot)
{
    const auto new_snapshot_id = getInteger(*new_snapshot, f_metadata_snapshot_id, "Snapshot");
    if (findSnapshot(metadata, new_snapshot_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Snapshot {} already exists", new_snapshot_id);

    /// The other fields the spec requires. The manifest list itself is not opened.
    getInteger(*new_snapshot, f_timestamp_ms, "Snapshot");
    getString(*new_snapshot, f_manifest_list, "Snapshot");
    const auto summary = getObject(*new_snapshot, f_summary, "Snapshot");
    getString(*summary, f_operation, "Snapshot summary");

    /// TODO: format version 3 also needs `first-row-id`, `added-rows` and `next-row-id` handling.
    const auto new_sequence_number = getInteger(*new_snapshot, f_metadata_sequence_number, "Snapshot");
    const auto last_sequence_number = getInteger(metadata, f_last_sequence_number, "Table metadata");
    if (new_sequence_number <= last_sequence_number)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Snapshot sequence number {} must be greater than {}", new_sequence_number, last_sequence_number);

    getArray(metadata, f_snapshots, "Table metadata")->add(new_snapshot);
    metadata.set(f_last_sequence_number, new_sequence_number);
}

void setSnapshotRef(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto ref_name = getString(update, "ref-name", "set-snapshot-ref");
    const auto type = getString(update, f_type, "set-snapshot-ref");
    if (type != f_branch && type != f_tag)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ref type must be 'branch' or 'tag', got '{}'", type);
    const auto snapshot_id = getInteger(update, f_metadata_snapshot_id, "set-snapshot-ref");
    const auto snapshot = findSnapshot(metadata, snapshot_id);
    if (!snapshot)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot set ref '{}': snapshot {} does not exist", ref_name, snapshot_id);

    Poco::JSON::Object::Ptr ref = new Poco::JSON::Object;
    ref->set(f_type, type);
    ref->set(f_metadata_snapshot_id, snapshot_id);
    /// Retention settings are stored but not acted on. Snapshot expiration is out of scope.
    for (const auto * key : {f_ref_max_ref_age_ms, f_ref_max_snapshot_age_ms, f_ref_min_snapshots_to_keep})
        if (update.has(key) && !update.isNull(key))
            ref->set(key, update.get(key));

    getObject(metadata, f_refs, "Table metadata")->set(ref_name, ref);

    /// `current-snapshot-id` and `snapshot-log` mirror the `main` branch for readers that do not know about refs.
    if (ref_name == f_main)
    {
        metadata.set(f_current_snapshot_id, snapshot_id);
        Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
        entry->set(f_metadata_snapshot_id, snapshot_id);
        entry->set(f_timestamp_ms, snapshot->get(f_timestamp_ms));
        getArray(metadata, f_snapshot_log, "Table metadata")->add(entry);
    }
}

void removeSnapshotRef(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto ref_name = getString(update, "ref-name", "remove-snapshot-ref");
    getObject(metadata, f_refs, "Table metadata")->remove(ref_name);
    if (ref_name == f_main)
        metadata.set(f_current_snapshot_id, -1);
}

void setProperties(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto updates = getObject(update, "updates", "set-properties");
    auto properties = getObject(metadata, f_properties, "Table metadata");
    for (const auto & [key, value] : *updates)
        properties->set(key, value.convert<String>());
}

void removeProperties(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto removals = getArray(update, "removals", "remove-properties");
    auto properties = getObject(metadata, f_properties, "Table metadata");
    for (unsigned i = 0; i < removals->size(); ++i)
        properties->remove(removals->get(i).convert<String>());
}

/// Spec actions this server knows about but does not apply yet.
const std::set<String> UNSUPPORTED_UPDATE_ACTIONS = {
    "assign-uuid", "upgrade-format-version", "add-schema", "set-current-schema", "add-spec", "set-default-spec",
    "add-sort-order", "set-default-sort-order", "remove-snapshots", "set-location", "set-statistics", "remove-statistics",
    "set-partition-statistics", "remove-partition-statistics", "remove-partition-specs", "remove-schemas",
    "add-encryption-key", "remove-encryption-key",
};

/// Records the previous metadata file in `metadata-log`, capped by `write.metadata.previous-versions-max`.
void appendMetadataLog(Poco::JSON::Object & metadata, const String & previous_metadata_location)
{
    Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
    entry->set(f_metadata_file, previous_metadata_location);
    entry->set(f_timestamp_ms, metadata.get(f_last_updated_ms));

    auto log = getArray(metadata, f_metadata_log, "Table metadata");
    log->add(entry);

    const auto properties = getObject(metadata, f_properties, "Table metadata");
    const auto max_entries = properties->optValue<size_t>("write.metadata.previous-versions-max", 100);
    while (log->size() > max_entries)
        log->remove(0);
}

}

std::optional<String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements)
{
    /// Requirement type -> {key in the requirement, key in the metadata}. These compare one integer each.
    static const std::map<String, std::pair<String, String>> integer_requirements = {
        {"assert-current-schema-id", {"current-schema-id", f_current_schema_id}},
        {"assert-last-assigned-field-id", {"last-assigned-field-id", f_last_column_id}},
        {"assert-last-assigned-partition-id", {"last-assigned-partition-id", f_last_partition_id}},
        {"assert-default-spec-id", {"default-spec-id", f_default_spec_id}},
        {"assert-default-sort-order-id", {"default-sort-order-id", f_default_sort_order_id}},
    };

    for (unsigned i = 0; i < requirements.size(); ++i)
    {
        const auto requirement = requirements.getObject(i);
        if (!requirement)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every requirement must be an object");
        const auto type = getString(*requirement, f_type, "Every requirement");

        if (type == "assert-create")
        {
            /// Stage-create is not supported, so the table always exists at this point.
            return "Table already exists";
        }
        else if (type == "assert-table-uuid")
        {
            const auto expected = getString(*requirement, "uuid", type);
            const auto actual = getString(metadata, f_table_uuid, "Table metadata");
            if (expected != actual)
                return fmt::format("Table uuid is {}, expected {}", actual, expected);
        }
        else if (type == "assert-ref-snapshot-id")
        {
            const auto ref = getString(*requirement, "ref", type);
            /// A missing or null `snapshot-id` means the ref must not exist. ClickHouse omits the key, pyiceberg sends null.
            std::optional<Int64> expected;
            if (requirement->has(f_metadata_snapshot_id) && !requirement->isNull(f_metadata_snapshot_id))
                expected = getInteger(*requirement, f_metadata_snapshot_id, type);
            const auto actual = maybeGetRefSnapshotId(metadata, ref);
            if (expected != actual)
                return fmt::format(
                    "Ref '{}' points to snapshot {}, expected {}",
                    ref,
                    actual ? toString(*actual) : "none",
                    expected ? toString(*expected) : "none");
        }
        else if (const auto it = integer_requirements.find(type); it != integer_requirements.end())
        {
            const auto & [request_key, metadata_key] = it->second;
            const auto expected = getInteger(*requirement, request_key, type);
            const auto actual = getInteger(metadata, metadata_key, "Table metadata");
            if (expected != actual)
                return fmt::format("{} is {}, expected {}", metadata_key, actual, expected);
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown requirement type '{}'", type);
        }
    }
    return std::nullopt;
}

Poco::JSON::Object::Ptr applyTableUpdates(
    const Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location)
{
    /// Round-trip through text is the simplest deep copy of a Poco JSON tree.
    auto result = parseJSONObject(toJSONString(metadata), "Table metadata");

    for (unsigned i = 0; i < updates.size(); ++i)
    {
        const auto update = updates.getObject(i);
        if (!update)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every update must be an object");
        const auto action = getString(*update, "action", "Every update");

        if (action == "add-snapshot")
            addSnapshot(*result, getObject(*update, "snapshot", action));
        else if (action == "set-snapshot-ref")
            setSnapshotRef(*result, *update);
        else if (action == "remove-snapshot-ref")
            removeSnapshotRef(*result, *update);
        else if (action == "set-properties")
            setProperties(*result, *update);
        else if (action == "remove-properties")
            removeProperties(*result, *update);
        else if (UNSUPPORTED_UPDATE_ACTIONS.contains(action))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Update '{}' is not supported", action);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown update action '{}'", action);
    }

    appendMetadataLog(*result, current_metadata_location);
    result->set(f_last_updated_ms, nowMs());
    return result;
}

String nextMetadataLocation(
    const Poco::JSON::Object & metadata, const String & current_metadata_location, const String & compression_suffix)
{
    const auto version = getMetadataFileAndVersion(current_metadata_location).version;
    return fmt::format(
        "{}/metadata/v{}-{}{}.metadata.json",
        getString(metadata, f_location, "Table metadata"),
        version + 1,
        toString(UUIDHelpers::generateV4()),
        compression_suffix);
}

}
