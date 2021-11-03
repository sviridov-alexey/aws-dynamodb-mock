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
            "SS" to attributeValue.ss()
        } else if (attributeValue.hasNs()) {
            "NS" to attributeValue.ns()
        } else if (attributeValue.hasBs()) {
            "BS" to attributeValue.bs()
        } else if (attributeValue.b() != null) {
            "B" to attributeValue.b().toString()
        } else if (attributeValue.hasM()) {
            "M" to attributeValue.m()
        } else if (attributeValue.hasL()) {
            "L" to attributeValue.l()
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
        require(request.tableName() !in tablesMetadata) {
            "Table ${request.tableName()} already exists. Cannot create."
        }
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

    fun putItem(request: PutItemRequest): PutItemResponse {
        val tableName = request.tableName()
        val tableMetadata = tablesMetadata[tableName]

        val partitionKeyName = tableMetadata?.partitionKey ?: return PutItemResponse.builder().build()
        val partitionKey = getKeyFromMetadata(partitionKeyName, request.item())

        val sortKey = getSortKeyFromMetadata(tableMetadata.sortKey, request.item())

        val itemsList = request.item().map { (k, v) ->
            val (type, value) = convertAttributeValueToInfo(v)
            requireNotNull(type) {"No such AttributeValue type"}
            requireNotNull(value) {"No such AttributeValue type"}

            AttributeInfo(k, type, value.toString())
        }.toList()

        val attributes = when (request.returnValues()) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey)).associate {
                    val (name, value) = convertAttributeInfoToValue(it)
                    name to value
                }
            }
            else -> null
        }

        if (attributes != null) {
            storage.updateItem(DBUpdateItemRequest(tableName, partitionKey, sortKey, itemsList))
        } else {
            storage.putItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        }
        return PutItemResponse.builder()
            .attributes(attributes)
            .build()
    }

    fun getItem(request: GetItemRequest): GetItemResponse {
        val tableName = request.tableName()
        val tableMetadata = tablesMetadata[tableName]

        val partitionKeyName = tableMetadata?.partitionKey ?: return GetItemResponse.builder().build()
        val partitionKey = getKeyFromMetadata(partitionKeyName, request.key())

        val sortKey = getSortKeyFromMetadata(tableMetadata.sortKey, request.key())

        val response = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))

        val item = response.filter { request.attributesToGet().contains(it.attributeName) }.associate {
            val (name, value) = convertAttributeInfoToValue(it)
            name to value
        }

        return GetItemResponse.builder()
            .item(item)
            .build()
    }

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse {
        val tableName = request.tableName()
        val tableMetadata = tablesMetadata[tableName]

        val partitionKeyName = tableMetadata?.partitionKey ?: return DeleteItemResponse.builder().build()
        val partitionKey = getKeyFromMetadata(partitionKeyName, request.key())

        val sortKey = getSortKeyFromMetadata(tableMetadata.sortKey, request.key())

        val attributes = when (request.returnValues()) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey)).associate {
                    val (name, value) = convertAttributeInfoToValue(it)
                    name to value
                }
            }
            else -> null
        }

        storage.deleteItem(DBDeleteItemRequest(tableName, partitionKey, sortKey))

        return DeleteItemResponse.builder()
            .attributes(attributes)
            .build()
    }

    private fun getKeyFromMetadata(partitionKeyName: String, keys: Map<String, AttributeValue>): Key {
        val partitionKeyAttributeValue = checkNotNull(keys[partitionKeyName])
        val (partitionKeyType, partitionKeyValue) = convertAttributeValueToInfo(partitionKeyAttributeValue)

        requireNotNull(partitionKeyType) {"No such AttributeValue type"}
        requireNotNull(partitionKeyValue) {"No such AttributeValue type"}

        return when (partitionKeyType) {
            "N" -> NumKey(partitionKeyName, partitionKeyValue.toString().toBigDecimal())
            else -> StringKey(partitionKeyName, partitionKeyValue.toString())
        }
    }

    private fun getSortKeyFromMetadata(sortKeyName: String?, keys: Map<String, AttributeValue>): Key? =
        if (sortKeyName != null) {
            val sortKeyAttributeValue = keys[sortKeyName]
            if (sortKeyAttributeValue == null) {
                null
            } else {
                getKeyFromMetadata(sortKeyName, keys)
            }
        } else null

}