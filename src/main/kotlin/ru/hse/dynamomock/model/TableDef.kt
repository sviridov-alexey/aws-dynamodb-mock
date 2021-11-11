package ru.hse.dynamomock.model

import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

data class TableMetadata(
    val tableName: String,
    val attributeDefinitions: List<AttributeDefinition>, // TODO store AttributeInfo
    val partitionKey: String,
    val sortKey: String?,
    val tableStatus: TableStatus,
    val creationDateTime: Instant = Instant.now()
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
        .creationDateTime(creationDateTime)
        .build()
}

fun CreateTableRequest.toTableMetadata(): TableMetadata = TableMetadata(
    tableName(),
    attributeDefinitions(),
    getPartitionKey(keySchema()),
    getSortKey(keySchema()),
    TableStatus.ACTIVE
)

private val KeySchemaElement.isPartition get(): Boolean = keyType() == KeyType.HASH
private val KeySchemaElement.isSort get(): Boolean = keyType() == KeyType.RANGE

private fun getPartitionKey(keySchema: List<KeySchemaElement>) =
    keySchema.first { it.isPartition }.attributeName()

private fun getSortKey(keySchema: List<KeySchemaElement>) =
    keySchema.firstOrNull { it.isSort }?.attributeName()

private fun nameToKeySchemaElement(name: String, keyType: KeyType) =
    KeySchemaElement.builder().attributeName(name).keyType(keyType).build()
