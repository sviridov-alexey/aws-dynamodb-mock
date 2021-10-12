package ru.hse.dynamomock.model

import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

data class TableMetadata(
    val tableName: String,
    val attributeDefinitions: List<AttributeDefinition>,
    val partitionKey: String,
    val sortKey: String?,
    val tableStatus: TableStatus,
    val creationDateTime: Instant = Instant.now()
) {
    companion object {
        fun TableMetadata.toTableDescription(): TableDescription {
            // TODO supports other parameters
            return TableDescription.builder()
                .tableName(tableName)
                .attributeDefinitions(attributeDefinitions)
                .keySchema(listOfNotNull(partitionKey.toKey(KeyType.HASH), sortKey?.toKey(KeyType.RANGE)))
                .tableStatus(tableStatus)
                .creationDateTime(creationDateTime)
                .build()
        }

        fun CreateTableRequest.toTableMetadata(): TableMetadata = TableMetadata(
            tableName(),
            attributeDefinitions(),
            keySchema().partitionKey,
            keySchema().sortKey,
            TableStatus.ACTIVE
        )

        private val KeySchemaElement.isPartition get(): Boolean = keyType() === KeyType.HASH
        private val KeySchemaElement.isSort get(): Boolean = keyType() === KeyType.RANGE
        private val List<KeySchemaElement>.partitionKey get(): String = first { it.isPartition }.attributeName()
        private val List<KeySchemaElement>.sortKey get(): String? = firstOrNull { it.isSort }?.attributeName()

        private fun String.toKey(keyType: KeyType): KeySchemaElement = KeySchemaElement.builder()
            .attributeName(this)
            .keyType(keyType)
            .build()
    }
}