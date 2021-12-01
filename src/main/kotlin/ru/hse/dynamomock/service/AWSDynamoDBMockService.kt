package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.query.retrieveAttributesTransformer
import ru.hse.dynamomock.model.query.retrieveFilterExpression
import ru.hse.dynamomock.model.query.retrieveKeyConditions
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.*

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val tablesMetadata = mutableMapOf<String, TableMetadata>()

    private fun toKey(keyName: String, attributeValue: AttributeValue): Pair<String, Key> =
        if (attributeValue.s() != null) {
            "S" to StringKey(keyName, attributeValue.s())
        } else if (attributeValue.n() != null) {
            "N" to NumKey(keyName, attributeValue.n().toBigDecimal())
        } else if (attributeValue.b() != null) {
            "B" to StringKey(keyName, Base64.getEncoder().encodeToString(attributeValue.b().asByteArray()))
        } else {
            throw DynamoDbException.builder().message("Member must satisfy enum value set: [B, N, S]").build()
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

    fun describeTable(request: DescribeTableRequest): TableDescription {
        val name = request.tableName()
        require(name in tablesMetadata) {
            "Cannot describe non-existent table $name."
        }
        return tablesMetadata.getValue(name).toTableDescription()
    }

    // TODO support exclusiveStartKey and limit
    // TODO take into account that it's impossible to use expression-like and not-expression-like at the same query
    fun query(request: QueryRequest): QueryResponse {
        require(request.indexName() == null) {
            "Indexes are not supported in query yet."
        }
        require(request.consistentRead() != false) {
            "Non-consistent read is not supported in query yet."
        }
        val tableName = request.tableName()
        require(tableName in tablesMetadata) {
            "Cannot query from non-existent table $tableName."
        }
        val table = tablesMetadata.getValue(tableName)

        val keyConditions = request.retrieveKeyConditions()
        val items = storage.query(tableName, keyConditions).let {
            if (request.scanIndexForward() != false) it else it.reversed()
        }.map { item ->
            item.associate { it.name to it.type.toAttributeValue() }
        }

        val filterExpression = request.retrieveFilterExpression()
        val filteredItems = filterExpression?.let {
            items.filter {
                val withoutKeys = it.toMutableMap().apply {
                    remove(table.partitionKey)
                    remove(table.sortKey)
                }
                filterExpression.evaluate(withoutKeys)
            }
        } ?: items

        val responseBuilder = QueryResponse.builder()
            .count(filteredItems.size)
            .scannedCount(items.size)
        return if (request.select() == Select.COUNT) {
            responseBuilder.build()
        } else {
            val transformer = request.retrieveAttributesTransformer()
            responseBuilder.items(filteredItems.map(transformer)).build()
        }
    }

    fun putItem(request: PutItemRequest): PutItemResponse {
        val tableName = request.tableName()

        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.item())

        val itemsList = request.item().map { (k, v) -> AttributeInfo(k, v.toAttributeTypeInfo()) }

        val attributes = getAttributesFromReturnValues(
            request.returnValues(),
            DBGetItemRequest(tableName, partitionKey, sortKey)
        )

        if (attributes != null) {
            storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
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

        val item = response?.filter { request.attributesToGet().contains(it.name) }?.associate {
            it.name to it.type.toAttributeValue()
        }

        return GetItemResponse.builder()
            .item(item)
            .build()
    }

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse {
        val tableName = request.tableName()
        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.key())

        val attributes = getAttributesFromReturnValues(
            request.returnValues(),
            DBGetItemRequest(tableName, partitionKey, sortKey)
        )

        storage.deleteItem(DBDeleteItemRequest(tableName, partitionKey, sortKey))

        return DeleteItemResponse.builder()
            .attributes(attributes)
            .build()
    }

    fun batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): BatchWriteItemResponse {
        if (!batchWriteItemRequest.hasRequestItems() || batchWriteItemRequest.requestItems().isEmpty()) {
            throw DynamoDbException.builder()
                .message("BatchWriteItem cannot have a null or no requests set")
                .build()
        }
        val requestItems = batchWriteItemRequest.requestItems()
        val putItemRequests = mutableListOf<DBPutItemRequest>()
        val deleteItemRequests = mutableListOf<DBDeleteItemRequest>()

        requestItems.keys.forEach{
            tablesMetadata[it] ?: throw ResourceNotFoundException.builder()
                .message("Cannot do operations on a non-existent table").build()
        }

        val batchSize = requestItems.entries.sumOf { it.value.size }

        if (batchSize > 25) {
            throw DynamoDbException.builder()
                .message("Too many items requested for the BatchWriteItem call")
                .build()
        }

        requestItems.entries.forEach{ tableRequests ->
            val items = mutableSetOf<Map<String, AttributeValue>>()
            val tableName = tableRequests.key
            tableRequests.value.forEach{
                val putRequest = it.putRequest()
                val deleteRequest = it.deleteRequest()
                if (putRequest == null && deleteRequest == null) {
                    throw DynamoDbException.builder()
                        .message("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
                        .build()
                } else if (putRequest != null && putRequest.hasItem() && (deleteRequest == null || !deleteRequest.hasKey())) {
                    val (partitionKey, sortKey) = getRequestMetadata(tableName, putRequest.item())
                    val keys = putRequest.item().filter { item ->
                        item.key == partitionKey.attributeName ||  item.key == sortKey?.attributeName
                    }
                    if (items.contains(keys)) {
                        throw DynamoDbException.builder()
                            .message("Provided list of item keys contains duplicates")
                            .build()
                    }
                    items.add(keys)
                    val itemsList = putRequest.item().map { (k, v) -> AttributeInfo(k, v.toAttributeTypeInfo()) }
                    putItemRequests.add(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
                } else if (deleteRequest != null && deleteRequest.hasKey() && (putRequest == null || !putRequest.hasItem())) {
                    val (partitionKey, sortKey) = getRequestMetadata(tableName, deleteRequest.key())

                    val keys = deleteRequest.key()
                    if (items.contains(keys)) {
                        throw DynamoDbException.builder()
                            .message("Provided list of item keys contains duplicates")
                            .build()
                    }

                    items.add(keys)
                    deleteItemRequests.add(DBDeleteItemRequest(tableName, partitionKey, sortKey))
                } else {
                    throw DynamoDbException.builder()
                        .message("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
                        .build()
                }
            }

        }

        putItemRequests.forEach{
            val attributes = getAttributesFromReturnValues(
                ReturnValue.ALL_OLD,
                DBGetItemRequest(it.tableName, it.partitionKey, it.sortKey)
            )
            if (attributes == null) {
                storage.putItem(it)
            } else {
                storage.updateItem(it)
            }
        }
        deleteItemRequests.forEach{ storage.deleteItem(it) }

        return BatchWriteItemResponse.builder()
            .build()
    }

    private fun getKeyFromMetadata(keyName: String, keys: Map<String, AttributeValue>, attributeDefinitions: List<AttributeDefinition>): Key {
        val keyAttributeValue =
            keys[keyName] ?: throw DynamoDbException
                .builder()
                .message("One of the required keys was not given a value")
                .build()
        val (keyType, key) = toKey(keyName, keyAttributeValue)
        val expectedKeyType =
            attributeDefinitions.firstOrNull { it.attributeName() == keyName } ?: throw DynamoDbException.builder()
                .message("One of the required keys was not given a value")
                .build()
        if (expectedKeyType.attributeTypeAsString().uppercase() != keyType) {
            throw DynamoDbException.builder()
                .message("Invalid attribute value type")
                .build()
        }
        return key
    }

    private fun getSortKeyFromMetadata(sortKeyName: String?, keys: Map<String, AttributeValue>, attributeDefinitions: List<AttributeDefinition>): Key? {
        sortKeyName ?: return null
        val sortKeyAttributeValue = keys[sortKeyName]
        sortKeyAttributeValue ?: return null

        return getKeyFromMetadata(sortKeyName, keys, attributeDefinitions)
    }

    private fun getRequestMetadata(tableName: String, keys: Map<String, AttributeValue>): Pair<Key, Key?> {
        val tableMetadata = tablesMetadata[tableName] ?: throw ResourceNotFoundException.builder()
            .message("Cannot do operations on a non-existent table").build()

        val partitionKeyName = tableMetadata.partitionKey
        return getKeyFromMetadata(partitionKeyName, keys, tableMetadata.attributeDefinitions) to
            getSortKeyFromMetadata(tableMetadata.sortKey, keys, tableMetadata.attributeDefinitions)
    }

    private fun getAttributesFromReturnValues(returnValues: ReturnValue?, request: DBGetItemRequest): Map<String, AttributeValue>? =
        when (returnValues) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(request)?.associate {
                    it.name to it.type.toAttributeValue()
                }
            }
            ReturnValue.NONE, null -> null
            else -> throw DynamoDbException.builder().message("Return values set to invalid value").build()
        }

}