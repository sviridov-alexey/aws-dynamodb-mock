package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.*
import software.amazon.awssdk.services.dynamodb.model.*

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val tablesMetadata = mutableMapOf<String, TableMetadata>()

    private fun convertAttributeValueToInfo(attributeValue: AttributeValue): Pair<String?, Any?> =
        if (attributeValue.bool() != null) {
            "BOOL" to attributeValue.bool().toString()
        } else if (attributeValue.s() != null) {
            "S" to attributeValue.s()
        } else if (attributeValue.n() != null) {
            "N" to attributeValue.n()
        } else if (attributeValue.hasSs()) {
            "SS" to attributeValue.ss().toString()
        } else if (attributeValue.hasNs()) {
            "NS" to attributeValue.ns().toString()
        } else if (attributeValue.hasBs()) {
            "BS" to attributeValue.bs().toString()
        } else if (attributeValue.b() != null) {
            "B" to attributeValue.b().toString()
        } else if (attributeValue.hasM()) {
            "M" to attributeValue.m().toString()
        } else if (attributeValue.hasL()) {
            "L" to attributeValue.l().toString()
        } else if (attributeValue.nul() != null) {
            "NUL" to attributeValue.nul().toString()
        } else {
            null to null
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

    fun createTable(request: CreateTableRequest): TableDescription {
        return request.toTableMetadata().also {
            storage.createTable(it)
            tablesMetadata[it.tableName] = it
        }.toTableDescription()
    }

    fun deleteTable(request: DeleteTableRequest): TableDescription {
        val name = request.tableName()
        require(name in tablesMetadata) {
            "Cannot delete non-existent table."
        }
        storage.deleteTable(name)
        return tablesMetadata.remove(name)!!.toTableDescription()
    }

    fun putItem(request: PutItemRequest) {
        val tableName = request.tableName()
        val tableMetadata = tablesMetadata[tableName]

        val partitionKeyName = tableMetadata?.partitionKey ?: return
        val sortKeyName = tableMetadata.sortKey

        val partitionKeyAttributeValue = request.item()[partitionKeyName]
            ?: // TODO: angry dynamodb error message
            return

        val (partitionKeyType, partitionKeyValue) = convertAttributeValueToInfo(partitionKeyAttributeValue)

        if (partitionKeyType == null || partitionKeyValue == null) {
            // TODO: angry dynamodb error message
            return
        }
        val partitionKey = Key(partitionKeyName, partitionKeyType, partitionKeyValue)

        val itemsList = request.item().map { (k, v) ->
            val (type, value) = convertAttributeValueToInfo(v)
            if (type == null || value == null) {
                // TODO: angry dynamodb error message
                return
            }
            AttributeInfo(k, type, value.toString())
        }.toList()

        val sortKey = if (sortKeyName != null) {
            val sortKeyAttributeValue = request.item()[tableMetadata.sortKey]
            if (sortKeyAttributeValue == null) {
                null
            } else {
                val (sortKeyType, sortKeyValue) = convertAttributeValueToInfo(sortKeyAttributeValue)
                if (sortKeyType == null || sortKeyValue == null) {
                    null
                } else {
                    Key(sortKeyName, sortKeyType, sortKeyValue)
                }
            }
        } else null

        storage.putItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
    }

    fun getItem(request: GetItemRequest): GetItemResponse {
        val tableName = request.tableName()
        val tableMetadata = tablesMetadata[tableName]
        val partitionKeyName = tableMetadata?.partitionKey ?: return GetItemResponse.builder().build()
        val sortKeyName = tableMetadata.sortKey

        val partitionKeyAttributeValue = request.key()[partitionKeyName]
            ?: // TODO: angry dynamodb error message
            return GetItemResponse.builder().build()

        val (partitionKeyType, partitionKeyValue) = convertAttributeValueToInfo(partitionKeyAttributeValue)
        if (partitionKeyType == null || partitionKeyValue == null) {
            // TODO: angry dynamodb error message
            return GetItemResponse.builder().build()
        }
        val partitionKey = Key(partitionKeyName, partitionKeyType, partitionKeyValue)

        val sortKey = if (sortKeyName != null) {
            val sortKeyAttributeValue = request.key()[tableMetadata.sortKey]
            if (sortKeyAttributeValue == null) {
                null
            } else {
                val (sortKeyType, sortKeyValue) = convertAttributeValueToInfo(sortKeyAttributeValue)
                if (sortKeyType == null || sortKeyValue == null) {
                    null
                } else {
                    Key(sortKeyName, sortKeyType, sortKeyValue)
                }
            }
        } else null

        val response = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey, request.attributesToGet()))

        val item = mutableMapOf<String, AttributeValue>()
        response.filter { request.attributesToGet().contains(it.attributeName)}.forEach {
            val (name, value) = convertAttributeInfoToValue(it)
            item[name] = value
        }
        return GetItemResponse.builder()
            .item(item)
            .build()
    }
}