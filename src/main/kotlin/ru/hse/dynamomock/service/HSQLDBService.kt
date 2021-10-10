package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.HSQLDBStorage
import ru.hse.dynamomock.db.util.AttributeInfo
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.TableStatus

class HSQLDBService {

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

    fun createTable(request: CreateTableRequest, dataStorageLayer: HSQLDBStorage): TableMetadata {
        val tableMetadata = TableMetadata(
            request.tableName(),
            request.attributeDefinitions(),
            request.keySchema(),
            TableStatus.ACTIVE
        )

        dataStorageLayer.createTable(tableMetadata)
        return tableMetadata
    }
    fun putItem(request: PutItemRequest, dataStorageLayer: HSQLDBStorage) {
        val tableName = request.tableName()
        val itemsList = mutableListOf<AttributeInfo>()
        request.item().forEach{
            itemsList.add(convertAttributeValueToInfo(it.key, it.value))
        }
        dataStorageLayer.putItem(HSQLDBPutItemRequest(tableName, itemsList))
    }

    fun getItem(request: GetItemRequest, dataStorageLayer: HSQLDBStorage): GetItemResponse {
        val tableName = request.tableName()
        val keys = request.key()
        if (keys.size > 1) {
            // TODO: sort key
        }

        val partitionKey = convertAttributeValueToInfo(keys.toList().first().first,
            keys.toList().first().second)

        dataStorageLayer.getItem(HSQLDBGetItemRequest(tableName, partitionKey, request.attributesToGet()))

        return GetItemResponse.builder()
            .build()
    }
}