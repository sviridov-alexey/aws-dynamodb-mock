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
    val localSecondaryIndexes: Map<String, LocalSecondaryIndex>,
    val creationDateTime: Instant = Instant.now(),
) {
    fun localSecondaryIndex(name: String): LocalSecondaryIndex {
        return localSecondaryIndexes[name] ?: throw dynamoException("Cannot find index $name in table $tableName.")
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
        .localSecondaryIndexes(localSecondaryIndexes.map { toLSIndexDescription(it.value) })
        .creationDateTime(creationDateTime)
        .build()
}

fun CreateTableRequest.toTableMetadata(): TableMetadata {
    checkKeySchema(keySchema())
    return TableMetadata(
        tableName(),
        attributeDefinitions(),
        getPartitionKey(keySchema()),
        getSortKey(keySchema()),
        TableStatus.ACTIVE,
        checkLocalSecondaryIndexes(localSecondaryIndexes(), keySchema(), attributeDefinitions())
    )
}


private val KeySchemaElement.isPartition get(): Boolean = keyType() == KeyType.HASH
private val KeySchemaElement.isSort get(): Boolean = keyType() == KeyType.RANGE

private fun getPartitionKey(keySchema: List<KeySchemaElement>) =
    keySchema.first { it.isPartition }.attributeName()

private fun getSortKey(keySchema: List<KeySchemaElement>) =
    keySchema.firstOrNull { it.isSort }?.attributeName()

private fun checkLocalSecondaryIndexes(
    indexes: List<LocalSecondaryIndex>,
    tableKeys: List<KeySchemaElement>,
    attributeDefinitions: List<AttributeDefinition>
): Map<String, LocalSecondaryIndex> {
    if (indexes.isEmpty()) {
        dynamoRequires (tableKeys.size == attributeDefinitions.size) {
            "The number of attributes in key schema must match the number of attributes defined in attribute definitions."
        }
        return emptyMap()
    }

    dynamoRequires(tableKeys.size > 1) {
        "Local Secondary indices are not allowed on hash tables, only hash and range tables"
    }

    val checkIndexName = "[a-zA-Z0-9-_.]+".toRegex()
    dynamoRequires(indexes.size <= 5) {
        "Cannot have more than 5 local secondary indexes per table"
    }
    val indexesMap = mutableMapOf<String, LocalSecondaryIndex>()
    val keySchemaElements = mutableSetOf<KeySchemaElement>()
    keySchemaElements.addAll(tableKeys)

    indexes.forEach {
        val name = it.indexName()
        dynamoRequires(!indexesMap.contains(name)) {
            "Two local secondary indices have the same name"
        }

        dynamoRequires(name != null && name.length in 3..255 && name.matches(checkIndexName)) {
            """
                Invalid table/index name.  Table/index names must be between 3 and 255 characters long, 
                and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'
            """.trimIndent()
        }

        dynamoRequires(it.projection() != null && it.projection().projectionType() != null) {
            "Indexes must have a projection specified"
        }

        dynamoRequires(it.hasKeySchema()) {
            "No defined key schema.  A key schema containing at least a hash key must be defined for all tables"
        }

        checkKeySchema(it.keySchema())

        dynamoRequires(it.keySchema().size == 2) {
            "Local secondary indices must have a range key"
        }

        val primaryKey = it.keySchema()[0]

        if (getPartitionKey(tableKeys) != primaryKey.attributeName()) {
            throw dynamoException(
                "Local Secondary indices must have the same hash key as the main table"
            )

        }
        keySchemaElements.addAll(it.keySchema())
        indexesMap[name] = it
    }

    dynamoRequires (keySchemaElements.size == attributeDefinitions.size) {
        "The number of attributes in key schema must match the number of attributes defined in attribute definitions."
    }

    return indexesMap
}

private fun checkKeySchema(keySchema: List<KeySchemaElement>) {
    dynamoRequires(keySchema.size < 3) {
        "Key Schema too big. Key Schema must at most consist of the hash and range key of a table"
    }

    val filteredSchema = keySchema.filter { it.isPartition }

    dynamoRequires(filteredSchema.isNotEmpty()) {
        "No Hash Key specified in schema. All Dynamo DB tables must have exactly one hash key"
    }

    dynamoRequires(filteredSchema.size <= 1) {
        "Too many hash keys specified. All Dynamo DB tables must have exactly one hash key"
    }

    if (keySchema.size == 2) {
        dynamoRequires(keySchema[0].isPartition && keySchema[1].isSort) {
            "Invalid key order. Hash Key must be specified first in key schema, Range Key must be specifed second"
        }
    }
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
