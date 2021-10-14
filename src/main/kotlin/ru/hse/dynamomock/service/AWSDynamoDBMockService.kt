package ru.hse.dynamomock.service

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.Key
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.TableMetadata.Companion.toTableMetadata
import software.amazon.awssdk.services.dynamodb.model.*

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val nameToTableMetadata = mutableMapOf<String, TableMetadata>()

    private fun convertAttributeValueToInfo(attributeValue: AttributeValue?): Pair<String, Any?> {
        require(attributeValue != null)
        var type = ""
        var item: Any? = null
        if (attributeValue.bool() != null) {
            type = "BOOL"
            item = attributeValue.bool().toString()
        } else if (attributeValue.s() != null) {
            type = "S"
            item = attributeValue.s()
        } else if (attributeValue.n() != null) {
            type = "N"
            item = attributeValue.n()
        } else if (attributeValue.hasSs()) {
            type = "SS"
            item = attributeValue.ss().toString()
        } else if (attributeValue.hasNs()) {
            type = "NS"
            item = attributeValue.ns().toString()
        } else if (attributeValue.hasBs()) {
            type = "BS"
            item = attributeValue.bs().toString()
        } else if (attributeValue.b() != null) {
            type = "B"
            item = attributeValue.b().toString()
        } else if (attributeValue.hasM()) {
            type = "M"
            item = attributeValue.m().toString()
        } else if (attributeValue.hasL()) {
            type = "L"
            item = attributeValue.l().toString()
        } else if (attributeValue.nul() != null) {
            type = "NUL"
            item = attributeValue.nul().toString()
        }
        return Pair(type, item)
    }

    private fun convertAttributeInfoToValue(attributeInfo: AttributeInfo): Pair<String, AttributeValue> {
        return when (attributeInfo.attributeType) {
            "S" -> Pair(attributeInfo.attributeName, AttributeValue.builder().s(attributeInfo.attributeValue).build())
            "N" -> Pair(attributeInfo.attributeName, AttributeValue.builder().n(attributeInfo.attributeValue).build())
            "SS" -> Pair(attributeInfo.attributeName, AttributeValue.builder().ss(attributeInfo.attributeValue).build())
            "BOOL" -> Pair(attributeInfo.attributeName, AttributeValue.builder().bool(attributeInfo.attributeValue.toBoolean()).build())
            else -> Pair(attributeInfo.attributeName, AttributeValue.builder().s(attributeInfo.attributeValue).build())
        }
    }

    fun createTable(request: CreateTableRequest): TableMetadata {
        return request.toTableMetadata().also {
            storage.createTable(it)
            nameToTableMetadata[it.tableName] = it
        }
    }

    fun putItem(request: PutItemRequest) {
        val tableName = request.tableName()
        val itemsList = mutableListOf<AttributeInfo>()
        val tableMetadata = nameToTableMetadata[tableName]

        val partitionKeyName = tableMetadata?.partitionKey ?: return
        val sortKeyName = tableMetadata.sortKey

        val partitionKeyAttributeValue = request.item()[partitionKeyName]
            ?: // TODO: angry dynamodb error message
            return

        val (partitionKeyType, partitionKeyValue) = convertAttributeValueToInfo(partitionKeyAttributeValue)


        request.item().forEach{
            val (type, value) = convertAttributeValueToInfo(it.value)
            itemsList.add(AttributeInfo(it.key, type, value.toString()))
        }

        if (partitionKeyValue == null) {
            // TODO: angry dynamodb error message
            return
        }

        var sortKey: Key? = null

        if (sortKeyName != null) {
            val sortKeyAttributeValue = request.item()[tableMetadata.sortKey]
            val (sortKeyType, sortKeyValue) = convertAttributeValueToInfo(sortKeyAttributeValue)
            sortKey = Key(sortKeyName, sortKeyType, sortKeyValue)
        }

        val partitionKey = Key(partitionKeyName, partitionKeyType, partitionKeyValue)

        storage.putItem(HSQLDBPutItemRequest(tableName, partitionKey, sortKey, Json.encodeToString(itemsList) ))
    }

    fun getItem(request: GetItemRequest): GetItemResponse {
        val tableName = request.tableName()
        val tableMetadata = nameToTableMetadata[tableName]
        val partitionKeyName = tableMetadata?.partitionKey ?: return GetItemResponse.builder().build()
        val sortKeyName = tableMetadata.sortKey

        val partitionKeyAttributeValue = request.key()[partitionKeyName]
            ?: // TODO: angry dynamodb error message
            return GetItemResponse.builder().build()

        val (partitionKeyType, partitionKeyValue) = convertAttributeValueToInfo(partitionKeyAttributeValue)
        if (partitionKeyValue == null) {
            // TODO: angry dynamodb error message
            return GetItemResponse.builder().build()
        }

        var sortKey: Key? = null

        if (sortKeyName != null) {
            val sortKeyAttributeValue = request.key()[tableMetadata.sortKey]
            val (sortKeyType, sortKeyValue) = convertAttributeValueToInfo(sortKeyAttributeValue)
            sortKey = Key(sortKeyName, sortKeyType, sortKeyValue)
        }

        val partitionKey = Key(partitionKeyName, partitionKeyType, partitionKeyValue)

        val response = storage.getItem(HSQLDBGetItemRequest(tableName, partitionKey, sortKey, request.attributesToGet()))
            ?: return GetItemResponse.builder()
                .build()

        val item = mutableMapOf<String, AttributeValue>()
        response.items.filter { request.attributesToGet().contains(it.attributeName)}.forEach {
            val (name, value) = convertAttributeInfoToValue(it)
            item[name] = value
        }
        return GetItemResponse.builder()
            .item(item)
            .build()
    }
}