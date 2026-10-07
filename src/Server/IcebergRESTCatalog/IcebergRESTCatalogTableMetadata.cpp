#include <Server/IcebergRESTCatalog/IcebergRESTCatalogTableMetadata.h>

#include "config.h"

#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>
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

/// Validates the partition fields against `schema_ids` and sets their `field-id`. Returns the new `last-partition-id`.
/// With `reassign_field_ids` the ids are 1000, 1001, ... in order, like Java's `TableMetadata.newTableMetadata`.
/// Before the table exists no manifest references these ids, so the server owns them. This also rules out duplicates.
/// Otherwise a given `field-id` is kept, because a create commit sends back the ids from the staged metadata.
Int64 preparePartitionFields(
    const Poco::JSON::Object & spec, const std::set<Int64> & schema_ids, Int64 last_partition_id, bool reassign_field_ids)
{
    std::set<Int64> field_ids;
    for (auto & field : getSpecFields(spec, "partition-spec", schema_ids, {f_name, f_transform}))
    {
        Int64 field_id;
        if (reassign_field_ids || !field->has(f_field_id) || field->isNull(f_field_id))
            field_id = ++last_partition_id;
        else
            field_id = getInteger(*field, f_field_id, "'partition-spec' field");
        if (!field_ids.insert(field_id).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate field-id {} in 'partition-spec'", field_id);
        field->set(f_field_id, field_id);
        last_partition_id = std::max(last_partition_id, field_id);
    }
    return last_partition_id;
}

/// Returns true if the order has fields.
bool validateSortFields(const Poco::JSON::Object & order, const std::set<Int64> & schema_ids)
{
    const auto fields = getSpecFields(order, "write-order", schema_ids, {f_transform, f_direction, f_null_order});
    for (const auto & field : fields)
    {
        const auto direction = field->getValue<String>(f_direction);
        if (direction != "asc" && direction != "desc")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'write-order' direction must be 'asc' or 'desc', got '{}'", direction);
        const auto null_order = field->getValue<String>(f_null_order);
        if (null_order != "nulls-first" && null_order != "nulls-last")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'write-order' null-order must be 'nulls-first' or 'nulls-last', got '{}'", null_order);
    }
    return !fields.empty();
}

std::set<Int64> getFieldIds(const Poco::JSON::Object::Ptr & schema)
{
    std::set<Int64> ids;
    collectAndValidateNestedFields(Poco::Dynamic::Var(schema), ids);
    return ids;
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

/// The array element with `id_key == id`, or nullptr.
Poco::JSON::Object::Ptr findById(const Poco::JSON::Array & array, const String & id_key, Int64 id)
{
    for (unsigned i = 0; i < array.size(); ++i)
    {
        const auto element = array.getObject(i);
        if (element && element->has(id_key) && element->getValue<Int64>(id_key) == id)
            return element;
    }
    return nullptr;
}

/// Specs and sort orders bind to the current schema, like `PartitionSpec.bind` in Java.
std::set<Int64> getCurrentSchemaFieldIds(const Poco::JSON::Object & metadata, const String & what)
{
    const auto current_schema_id = getInteger(metadata, f_current_schema_id, "Table metadata");
    const auto schema = findById(*getArray(metadata, f_schemas, "Table metadata"), f_schema_id, current_schema_id);
    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} needs a current schema, set it first", what);
    return getFieldIds(schema);
}

/// The functions below apply one metadata update each. `buildInitialTableMetadata` and the create commit share them.

void assignUUID(Poco::JSON::Object & metadata, const String & uuid)
{
    UUID parsed;
    if (!tryParse(parsed, uuid))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a valid uuid", uuid);
    metadata.set(f_table_uuid, uuid);
}

/// The empty base is already version 2, so this only checks the value.
/// TODO: support format version 3. The file needs `next-row-id`, and snapshots need `first-row-id` and `added-rows`.
void upgradeFormatVersion(Int64 format_version)
{
    if (format_version != 2)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only format-version 2 is supported, got {}", format_version);
}

/// Checks the schema is a struct, assigns the next `schema-id` and returns it. Updates `last-column-id`.
/// The `last-column-id` of an `add-schema` update is deprecated in the spec and ignored: the field ids say the same.
/// TODO: reuse an identical existing schema instead of adding a copy, like Java's `TableMetadata.Builder.addSchema`.
Int64 addSchema(Poco::JSON::Object & metadata, Poco::JSON::Object::Ptr schema)
{
    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be an object");
    if (schema->optValue<String>(f_type, f_struct) != f_struct)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'schema' must be a struct type");
    schema->set(f_type, f_struct);
    const auto field_ids = getFieldIds(schema);

    auto schemas = getArray(metadata, f_schemas, "Table metadata");
    const Int64 schema_id = schemas->size();
    schema->set(f_schema_id, schema_id);
    schemas->add(schema);

    /// Never decreases.
    const Int64 highest_field_id = field_ids.empty() ? 0 : *field_ids.rbegin();
    metadata.set(f_last_column_id, std::max(getInteger(metadata, f_last_column_id, "Table metadata"), highest_field_id));
    return schema_id;
}

/// Assigns the next `spec-id` and returns it. Updates `last-partition-id`.
Int64 addPartitionSpec(Poco::JSON::Object & metadata, Poco::JSON::Object::Ptr spec, bool reassign_field_ids)
{
    spec = prepareSpec(spec);
    const auto last_partition_id = preparePartitionFields(
        *spec,
        getCurrentSchemaFieldIds(metadata, "'partition-spec'"),
        getInteger(metadata, f_last_partition_id, "Table metadata"),
        reassign_field_ids);

    auto specs = getArray(metadata, f_partition_specs, "Table metadata");
    const Int64 spec_id = specs->size();
    spec->set(f_spec_id, spec_id);
    specs->add(spec);
    metadata.set(f_last_partition_id, last_partition_id);
    return spec_id;
}

/// Returns the `order-id`. The spec reserves 0 for the unsorted order, so a sorted order gets 1 like in Java and pyiceberg.
Int64 addSortOrder(Poco::JSON::Object & metadata, Poco::JSON::Object::Ptr order)
{
    order = prepareSpec(order);
    const bool sorted = validateSortFields(*order, getCurrentSchemaFieldIds(metadata, "'write-order'"));
    auto orders = getArray(metadata, f_sort_orders, "Table metadata");

    Int64 order_id = 0;
    if (sorted)
    {
        for (unsigned i = 0; i < orders->size(); ++i)
            order_id = std::max(order_id, getInteger(*orders->getObject(i), f_order_id, "Sort order"));
        ++order_id;
    }
    else if (findById(*orders, f_order_id, 0))
    {
        /// There is one unsorted order per table.
        return 0;
    }

    order->set(f_order_id, order_id);
    orders->add(order);
    return order_id;
}

/// Sets `target_key` to `id` after checking that `array_key` has an element with that id.
void setDefaultId(Poco::JSON::Object & metadata, const String & array_key, const String & item_id_key, const String & target_key, Int64 id)
{
    if (!findById(*getArray(metadata, array_key, "Table metadata"), item_id_key, id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot set '{}': no element of '{}' has {} {}", target_key, array_key, item_id_key, id);
    metadata.set(target_key, id);
}

void setCurrentSchema(Poco::JSON::Object & metadata, Int64 schema_id)
{
    setDefaultId(metadata, f_schemas, f_schema_id, f_current_schema_id, schema_id);
}

void setDefaultSpec(Poco::JSON::Object & metadata, Int64 spec_id)
{
    setDefaultId(metadata, f_partition_specs, f_spec_id, f_default_spec_id, spec_id);
}

void setDefaultSortOrder(Poco::JSON::Object & metadata, Int64 order_id)
{
    setDefaultId(metadata, f_sort_orders, f_order_id, f_default_sort_order_id, order_id);
}

/// The handler checks the location against the warehouse bucket.
void setLocation(Poco::JSON::Object & metadata, const String & location)
{
    metadata.set(f_location, stripTrailingSlashes(location));
}

/// A table with no uuid, location, schema, spec or sort order. The base of a create commit.
Poco::JSON::Object::Ptr newEmptyTableMetadata()
{
    /// Key order follows the Iceberg spec listing so the file reads like the ones Java writes.
    /// Every key is set here, because `set` on an existing key keeps its position.
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    metadata->set(f_format_version, 2);
    metadata->set(f_table_uuid, "");
    metadata->set(f_location, "");
    metadata->set(f_last_sequence_number, 0);
    metadata->set(f_last_updated_ms, nowMs());
    metadata->set(f_last_column_id, -1);
    metadata->set(f_current_schema_id, -1);
    metadata->set(f_schemas, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_default_spec_id, -1);
    metadata->set(f_partition_specs, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_last_partition_id, PARTITION_FIELD_ID_START - 1);
    metadata->set(f_default_sort_order_id, -1);
    metadata->set(f_sort_orders, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata->set(f_properties, Poco::JSON::Object::Ptr(new Poco::JSON::Object));
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

/// A create commit must set everything a CreateTableRequest sets.
/// TODO: fall back to the unpartitioned spec and the unsorted order when the client sends none, like Java does.
void validateCreatedMetadata(const Poco::JSON::Object & metadata)
{
    if (getString(metadata, f_table_uuid, "Table metadata").empty() || getString(metadata, f_location, "Table metadata").empty()
        || getInteger(metadata, f_current_schema_id, "Table metadata") < 0 || getInteger(metadata, f_default_spec_id, "Table metadata") < 0
        || getInteger(metadata, f_default_sort_order_id, "Table metadata") < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "A create commit must set the table uuid, location, current schema, default spec and default sort order");
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

    auto metadata = newEmptyTableMetadata();
    assignUUID(*metadata, uuid);
    setLocation(*metadata, location);
    setCurrentSchema(*metadata, addSchema(*metadata, schema));
    setDefaultSpec(*metadata, addPartitionSpec(*metadata, partition_spec, /*reassign_field_ids*/ true));
    setDefaultSortOrder(*metadata, addSortOrder(*metadata, write_order));

    auto properties_json = getObject(*metadata, f_properties, "Table metadata");
    for (const auto & [key, value] : properties)
        properties_json->set(key, value);
    return metadata;
}

String initialMetadataLocation(const String & location, const String & uuid, const String & compression_suffix)
{
    /// Same naming as the ClickHouse Iceberg writer: `<location>/metadata/v<version>-<uuid>.metadata.json`, starting at 1.
    return fmt::format("{}/metadata/v1-{}{}.metadata.json", location, uuid, compression_suffix);
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

/// Spec actions this server knows about but does not apply to an existing table.
/// The first nine are applied only by a create commit, where the base is empty. On an existing table they would need
/// the schema evolution rules (no field id reuse, compatible type changes), which are not implemented.
const std::set<String> UNSUPPORTED_UPDATE_ACTIONS = {
    "assign-uuid", "upgrade-format-version", "add-schema", "set-current-schema", "add-spec", "set-default-spec",
    "add-sort-order", "set-default-sort-order", "set-location", "remove-snapshots", "set-statistics", "remove-statistics",
    "set-partition-statistics", "remove-partition-statistics", "remove-partition-specs", "remove-schemas",
    "add-encryption-key", "remove-encryption-key",
};

/// Ids of the elements a create commit added so far. `-1` in a `set-*` update means "the last one added".
struct LastAddedIds
{
    std::optional<Int64> schema_id;
    std::optional<Int64> spec_id;
    std::optional<Int64> sort_order_id;
};

Int64 resolveLastAdded(Int64 id, const std::optional<Int64> & last_added, const String & action)
{
    if (id != -1)
        return id;
    if (!last_added)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' refers to the last added element, but nothing was added", action);
    return *last_added;
}

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

bool hasAssertCreate(const Poco::JSON::Array & requirements)
{
    for (unsigned i = 0; i < requirements.size(); ++i)
    {
        const auto requirement = requirements.getObject(i);
        if (requirement && requirement->optValue<String>(f_type, "") == "assert-create")
            return true;
    }
    return false;
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
            /// The handler runs this only for an existing table. A create commit takes the other path.
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

namespace
{

/// `creating` enables the updates that describe a new table. They are applied to the empty base only.
void applyUpdates(Poco::JSON::Object & result, const Poco::JSON::Array & updates, bool creating)
{
    LastAddedIds last_added;

    for (unsigned i = 0; i < updates.size(); ++i)
    {
        const auto update = updates.getObject(i);
        if (!update)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every update must be an object");
        const auto action = getString(*update, "action", "Every update");

        if (action == "add-snapshot")
            addSnapshot(result, getObject(*update, "snapshot", action));
        else if (action == "set-snapshot-ref")
            setSnapshotRef(result, *update);
        else if (action == "remove-snapshot-ref")
            removeSnapshotRef(result, *update);
        else if (action == "set-properties")
            setProperties(result, *update);
        else if (action == "remove-properties")
            removeProperties(result, *update);
        else if (creating && action == "assign-uuid")
            assignUUID(result, getString(*update, "uuid", action));
        else if (creating && action == "upgrade-format-version")
            upgradeFormatVersion(getInteger(*update, f_format_version, action));
        else if (creating && action == "add-schema")
            last_added.schema_id = addSchema(result, getObject(*update, "schema", action));
        else if (creating && action == "set-current-schema")
            setCurrentSchema(result, resolveLastAdded(getInteger(*update, f_schema_id, action), last_added.schema_id, action));
        else if (creating && action == "add-spec")
            last_added.spec_id = addPartitionSpec(result, getObject(*update, "spec", action), /*reassign_field_ids*/ false);
        else if (creating && action == "set-default-spec")
            setDefaultSpec(result, resolveLastAdded(getInteger(*update, f_spec_id, action), last_added.spec_id, action));
        else if (creating && action == "add-sort-order")
            last_added.sort_order_id = addSortOrder(result, getObject(*update, "sort-order", action));
        else if (creating && action == "set-default-sort-order")
            setDefaultSortOrder(result, resolveLastAdded(getInteger(*update, "sort-order-id", action), last_added.sort_order_id, action));
        else if (creating && action == "set-location")
            setLocation(result, getString(*update, f_location, action));
        else if (UNSUPPORTED_UPDATE_ACTIONS.contains(action))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Update '{}' is not supported", action);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown update action '{}'", action);
    }

    result.set(f_last_updated_ms, nowMs());
}

}

Poco::JSON::Object::Ptr applyTableUpdates(
    const Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location)
{
    /// Round-trip through text is the simplest deep copy of a Poco JSON tree.
    auto result = parseJSONObject(toJSONString(metadata), "Table metadata");
    applyUpdates(*result, updates, /*creating*/ false);
    appendMetadataLog(*result, current_metadata_location);
    return result;
}

Poco::JSON::Object::Ptr buildTableMetadataFromUpdates(const Poco::JSON::Array & updates)
{
    auto result = newEmptyTableMetadata();
    applyUpdates(*result, updates, /*creating*/ true);
    validateCreatedMetadata(*result);
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
