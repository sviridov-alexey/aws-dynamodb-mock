package ru.hse.dynamomock.model

import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

data class TableMetadata(
    val tableName: String,
    val attributeDefinitions: List<AttributeDefinition>, // TODO store AttributeInfo
    val partitionKey: String,
    val sortKey: String?,
    val tableStatus: TableStatus,
    val localSecondaryIndexes: List<LocalSecondaryIndex>,
    val creationDateTime: Instant = Instant.now(),
) {
    fun localSecondaryIndex(name: String): LocalSecondaryIndex {
        return localSecondaryIndexes.firstOrNull { it.indexName() == name }
            ?: throw dynamoException("Cannot find index $name in table $tableName.")
    }

    // TODO supports other parameters
    fun toTableDescription(): TableDescription = TableDescription.builder()
        .tableName(tableName)
        .attributeDefinitions(attributeDefinitions)
        .keySchema(listOfNotNull(
            nameToKeySchemaElement(partitionKey, KeyType.HASH),
            sortKey?.let { nameToKeySchemaElement(it, KeyType.RANGE) }
        ))
        .tableStatus(tableStatus)
        .localSecondaryIndexes(localSecondaryIndexes.map { toLSIndexDescription(it) })
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
    val checkIndexName = "[a-zA-Z0-9-_.]+".toRegex()
    dynamoRequires(indexes.size <= 5) {
        "Cannot have more than 5 local secondary indexes per table"
    }
    val indexesNames = mutableSetOf<String>()
    indexes.forEach {
        val name = it.indexName()
        dynamoRequires(!indexesNames.contains(name)) {
            "Two local secondary indices have the same name"
        }

        dynamoRequires(name != null && name.length in 3..255 && name.matches(checkIndexName)) {
            """
                Invalid table/index name.  Table/index names must be between 3 and 255 characters long, 
                and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.
            """.trimIndent()
        }

        dynamoRequires(it.projection() != null) {
            "Indexes must have a projection specified"
        }

        dynamoRequires(it.hasKeySchema()) {
            "No defined key schema.  A key schema containing at least a hash key must be defined for all tables"
        }

        val primaryKey = it.keySchema().firstOrNull { key -> key.keyType() == KeyType.HASH }
            ?: throw dynamoException(
                "No Hash Key specified in schema.  All Dynamo DB tables must have exactly one hash key"
            )
        if (getPartitionKey(tableKeys) != primaryKey.attributeName()) {
            throw dynamoException(
                "Local Secondary indices must have the same hash key as the main table"
            )

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
