package ru.hse.dynamomock.model

import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

data class TableMetadata(
    val tableName: String,
    val attributeDefinitions: List<AttributeDefinition>, // TODO store AttributeInfo
    val partitionKey: String,
    val sortKey: String?,
    val tableStatus: TableStatus,
    val localSecondaryIndexes: List<LocalSecondaryIndex>?,
    val creationDateTime: Instant = Instant.now(),
) {
    // TODO supports other parameters
    fun toTableDescription(): TableDescription = TableDescription.builder()
        .tableName(tableName)
        .attributeDefinitions(attributeDefinitions)
        .keySchema(listOfNotNull(
            nameToKeySchemaElement(partitionKey, KeyType.HASH),
            sortKey?.let { nameToKeySchemaElement(it, KeyType.RANGE) }
        ))
        .tableStatus(tableStatus)
        .localSecondaryIndexes(localSecondaryIndexes?.map { toLSIndexDescription(it) })
        .creationDateTime(creationDateTime)
        .build()
}

fun CreateTableRequest.toTableMetadata(): TableMetadata = TableMetadata(
    tableName(),
    attributeDefinitions(),
    getPartitionKey(keySchema()),
    getSortKey(keySchema()),
    TableStatus.ACTIVE,
    checkLocalSecondaryIndexes(localSecondaryIndexes(), keySchema())
)

private val KeySchemaElement.isPartition get(): Boolean = keyType() == KeyType.HASH
private val KeySchemaElement.isSort get(): Boolean = keyType() == KeyType.RANGE

private fun getPartitionKey(keySchema: List<KeySchemaElement>) =
    keySchema.first { it.isPartition }.attributeName()

private fun getSortKey(keySchema: List<KeySchemaElement>) =
    keySchema.firstOrNull { it.isSort }?.attributeName()

private fun checkLocalSecondaryIndexes(indexes: List<LocalSecondaryIndex>, tableKeys: List<KeySchemaElement>): List<LocalSecondaryIndex> {
    if (indexes.size > 5) {
        throw DynamoDbException.builder()
            .message("Cannot have more than 5 local secondary indexes per table")
            .build()
    }
    val indexesNames = mutableSetOf<String>()
    indexes.forEach {
        val name = it.indexName()
        if (indexesNames.contains(name)) {
            throw DynamoDbException.builder()
                .message("Two local secondary indices have the same name")
                .build()
        }
        if (name == null || !(name.length in 3..255 && name.matches("[a-zA-Z0-9-_.]+".toRegex()))) {
            throw DynamoDbException.builder()
                .message("""
                    Invalid table/index name.  Table/index names must be between 3 and 255 characters long, 
                    and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.
                """.trimIndent()
                )
                .build()
        }
        if (it.projection() == null) {
            throw DynamoDbException.builder()
                .message("Indexes must have a projection specified")
                .build()
        }
        if (!it.hasKeySchema()) {
            throw DynamoDbException.builder()
                .message("No defined key schema.  A key schema containing at least a hash key must be defined for all tables")
                .build()
        }

        val primaryKey = it.keySchema().firstOrNull { key -> key.keyType() == KeyType.HASH }
            ?: throw DynamoDbException.builder()
                .message("No Hash Key specified in schema.  All Dynamo DB tables must have exactly one hash key")
                .build()
        if (getPartitionKey(tableKeys) != primaryKey.attributeName()) {
            throw DynamoDbException.builder()
                .message("Local Secondary indices must have the same hash key as the main table")
                .build()

        }
        indexesNames.add(name)
    }
    return indexes
}

private fun nameToKeySchemaElement(name: String, keyType: KeyType) =
    KeySchemaElement.builder().attributeName(name).keyType(keyType).build()

private fun toLSIndexDescription(index: LocalSecondaryIndex) =
    LocalSecondaryIndexDescription.builder()
        .indexName(index.indexName())
        .keySchema(index.keySchema())
        .projection(index.projection())
        // TODO: more parameters
    .build()
