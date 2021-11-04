package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.*
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.IllegalArgumentException
import java.util.Base64

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val tablesMetadata = mutableMapOf<String, TableMetadata>()

    private fun convertAttributeValueToInfo(attributeValue: AttributeValue): Pair<String, Any> =
        if (attributeValue.bool() != null) {
            "BOOL" to attributeValue.bool()
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
            "B" to Base64.getEncoder().encodeToString(attributeValue.b().asByteArray())
        } else if (attributeValue.hasM()) {
            "M" to attributeValue.m()
        } else if (attributeValue.hasL()) {
            "L" to attributeValue.l()
        } else if (attributeValue.nul() != null) {
            "NUL" to attributeValue.nul().toString()
        } else {
            throw IllegalArgumentException("No such AttributeValue type")
        }

    private fun convertAttributeInfoToValue(attributeInfo: AttributeInfo): Pair<String, AttributeValue> {
        return when (attributeInfo.attributeType) {
            "S" -> Pair(attributeInfo.attributeName, AttributeValue.builder().s(attributeInfo.attributeValue).build())
            "N" -> Pair(attributeInfo.attributeName, AttributeValue.builder().n(attributeInfo.attributeValue).build())
            "SS" -> Pair(attributeInfo.attributeName, AttributeValue.builder().ss(attributeInfo.attributeValue).build())
            "BOOL" -> Pair(attributeInfo.attributeName, AttributeValue.builder().bool(attributeInfo.attributeValue.toBoolean()).build())
            "B" -> Pair(attributeInfo.attributeName, AttributeValue.builder().b(SdkBytes.fromByteArray(Base64.getDecoder().decode(attributeInfo.attributeValue))).build())
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

        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.item())

        val itemsList = request.item().map { (k, v) ->
            val (type, value) = convertAttributeValueToInfo(v)
            AttributeInfo(k, type, value.toString())
        }

        val attributes = when (request.returnValues()) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
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
        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.key())

        val response = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))

        val item = response?.filter { request.attributesToGet().contains(it.attributeName) }?.associate {
            val (name, value) = convertAttributeInfoToValue(it)
            name to value
        }

        return GetItemResponse.builder()
            .item(item)
            .build()
    }

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse {
        val tableName = request.tableName()
        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.key())

        val attributes = when (request.returnValues()) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
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

        return when (partitionKeyType) {
            "N" -> NumKey(partitionKeyName, partitionKeyValue.toString().toBigDecimal())
            else -> StringKey(partitionKeyName, partitionKeyValue.toString())
        }
    }

    private fun getSortKeyFromMetadata(sortKeyName: String?, keys: Map<String, AttributeValue>): Key? {
        sortKeyName ?: return null
        val sortKeyAttributeValue = keys[sortKeyName]
        sortKeyAttributeValue ?: return null

        return getKeyFromMetadata(sortKeyName, keys)
    }

    private fun getRequestMetadata(tableName: String, keys: Map<String, AttributeValue>): Pair<Key, Key?> {
        val tableMetadata = tablesMetadata.getValue(tableName)

        val partitionKeyName = checkNotNull(tableMetadata.partitionKey)
        return getKeyFromMetadata(partitionKeyName, keys) to getSortKeyFromMetadata(tableMetadata.sortKey, keys)
    }
}