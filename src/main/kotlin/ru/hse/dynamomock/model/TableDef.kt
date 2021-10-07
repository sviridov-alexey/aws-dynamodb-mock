package ru.hse.dynamomock.model

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import java.time.Instant

data class TableMetadata(
    val tableName: String,
    val attributeDefinitions: List<AttributeDefinition>,
    val keySchema: List<KeySchemaElement>,
    val tableStatus: TableStatus,
    val creationDateTime: Instant = Instant.now()
) {
    companion object {
        fun TableMetadata.toTableDescription(): TableDescription {
            // TODO supports other parameters
            return TableDescription.builder()
                .tableName(tableName)
                .attributeDefinitions(attributeDefinitions)
                .keySchema(keySchema)
                .tableStatus(tableStatus)
                .creationDateTime(creationDateTime)
                .build()
        }
    }
}