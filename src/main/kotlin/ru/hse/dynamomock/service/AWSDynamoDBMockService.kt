package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private fun convertAttributeValueToInfo(attributeName: String, attributeValue: AttributeValue): AttributeInfo {
        var type = ""
        var item: Any? = null
        if (attributeValue.bool() != null) {
            type = "Boolean"
            item = attributeValue.bool()
        } else if (attributeValue.s() != null) {
            type = "String"
            item = attributeValue.s()
        }
        // TODO: other types

        return AttributeInfo(attributeName, type, item)
    }

    fun createTable(request: CreateTableRequest): TableMetadata {
        val tableMetadata = TableMetadata(
            request.tableName(),
            request.attributeDefinitions(),
            request.keySchema(),
            TableStatus.ACTIVE
        )

        storage.createTable(tableMetadata)
        return tableMetadata
    }
    fun putItem(request: PutItemRequest) {
        val tableName = request.tableName()
        val itemsList = mutableListOf<AttributeInfo>()
        request.item().forEach{
            itemsList.add(convertAttributeValueToInfo(it.key, it.value))
        }
        storage.putItem(HSQLDBPutItemRequest(tableName, itemsList))
    }

    fun getItem(request: GetItemRequest): GetItemResponse {
        val tableName = request.tableName()
        val keys = request.key()
        if (keys.size > 1) {
            // TODO: sort key
        }

        val partitionKey = convertAttributeValueToInfo(keys.toList().first().first,
            keys.toList().first().second)

        storage.getItem(HSQLDBGetItemRequest(tableName, partitionKey, request.attributesToGet()))

        return GetItemResponse.builder()
            .build()
    }
}