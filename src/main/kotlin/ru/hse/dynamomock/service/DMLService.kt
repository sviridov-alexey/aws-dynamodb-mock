package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.DBDeleteItemRequest
import ru.hse.dynamomock.model.DBGetItemRequest
import ru.hse.dynamomock.model.DBPutItemRequest
import ru.hse.dynamomock.model.DynamoType
import ru.hse.dynamomock.model.Key
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.toAttributeTypeInfo
import ru.hse.dynamomock.model.utils.checkAttributeValue
import ru.hse.dynamomock.model.utils.getKeyFromMetadata
import ru.hse.dynamomock.model.utils.getSortKeyFromMetadata
import ru.hse.dynamomock.model.utils.itemToAttributeInfo
import software.amazon.awssdk.services.dynamodb.model.AttributeAction
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

class DMLService(
    private val storage: DataStorageLayer,
    private val tablesMetadata: MutableMap<String, TableMetadata>
) {

    private fun checkTableExists(tableName: String) =
        tablesMetadata[tableName] ?: throw ResourceNotFoundException.builder()
            .message("Cannot do operations on a non-existent table").build()

    private fun getRequestMetadata(tableName: String, keys: Map<String, AttributeValue>): Pair<Key, Key?> {
        val tableMetadata = checkTableExists(tableName)

        val partitionKeyName = tableMetadata.partitionKey
        return getKeyFromMetadata(partitionKeyName, keys, tableMetadata.attributeDefinitions) to
            getSortKeyFromMetadata(tableMetadata.sortKey, keys, tableMetadata.attributeDefinitions)
    }

    private fun getAttributesFromReturnValues(
        returnValues: ReturnValue?,
        prevItem: Map<String, AttributeValue>?
    ): Map<String, AttributeValue>? =
        when (returnValues) {
            ReturnValue.ALL_OLD -> prevItem
            ReturnValue.NONE, null -> null
            else -> throw dynamoException("Return values set to invalid value")
        }

    private fun checkNumOfKeys(tableName: String, keys: Map<String, AttributeValue>) {
        val tableMetadata = checkTableExists(tableName)
        val actualSize = if (tableMetadata.sortKey != null) 2 else 1
        if (keys.size > actualSize) {
            throw dynamoException("The number of conditions on the keys is invalid")
        }
    }

    fun putItem(request: PutItemRequest): PutItemResponse {
        val tableName = request.tableName()
        // check num of keys?????
        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.item())

        val itemsList = itemToAttributeInfo(request.item())

        val previousItem = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
            it.name to it.type.toAttributeValue()
        }

        if (previousItem != null) {
            storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        } else {
            storage.putItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        }

        val attributes = getAttributesFromReturnValues(request.returnValues(), previousItem)

        return PutItemResponse.builder()
            .attributes(attributes)
            .build()
    }

    fun getItem(request: GetItemRequest): GetItemResponse {
        val tableName = request.tableName()
        checkNumOfKeys(tableName, request.key())
        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.key())

        val response = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))

        val item = response?.filter { it.name in request.attributesToGet() }?.associate {
            it.name to it.type.toAttributeValue()
        }

        return GetItemResponse.builder()
            .item(item)
            .build()
    }

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse {
        val tableName = request.tableName()
        checkNumOfKeys(tableName, request.key())
        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.key())

        val previousItem = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
            it.name to it.type.toAttributeValue()
        }

        if (previousItem != null) {
            storage.deleteItem(DBDeleteItemRequest(tableName, partitionKey, sortKey))
        }

        val attributes = getAttributesFromReturnValues(request.returnValues(), previousItem)

        return DeleteItemResponse.builder()
            .attributes(attributes)
            .build()
    }

    private fun buildItem(
        keys: Map<String, AttributeValue>,
        columnName: String,
        attributeValue: AttributeValue
    ): Map<String, AttributeValue> = mapOf(columnName to attributeValue) + keys

    private fun checkAttributes(
        name: String,
        allowedTypes: List<DynamoType>,
        first: AttributeValue,
        second: AttributeValue? = null
    ): DynamoType {
        val firstType = checkAttributeValue(first)
        val secondType = if (second != null) checkAttributeValue(second) else firstType
        if (secondType !in allowedTypes) {
            val errorString =
                "One or more parameter values were invalid: $name action with value is not supported for the type " +
                    when (secondType) {
                        DynamoType.N -> "NUMBER"
                        DynamoType.S -> "STRING"
                        DynamoType.B -> "BINARY"
                        DynamoType.NS -> "NUMBER SET"
                        DynamoType.SS -> "STRING SET"
                        DynamoType.BS -> "BINARY SET"
                        DynamoType.BOOL -> "BOOLEAN"
                        DynamoType.NULL -> "NULL"
                        DynamoType.L -> "LIST"
                        DynamoType.M -> "MAP"
                    }
            throw dynamoException(errorString)
        }
        if (firstType != secondType) {
            throw dynamoException("Type mismatch for attribute to update")
        }
        return secondType
    }

    private fun putInUpdate(
        tableName: String, partitionKey: Key, sortKey: Key?,
        columnName: String, av: AttributeValueUpdate, keys: Map<String, AttributeValue>
    ) {
        val previousItem = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
            it.name to it.type.toAttributeValue()
        }
        if (previousItem != null) {
            val newItem = previousItem.toMutableMap()

            newItem[columnName] = av.value()
            val itemsList = itemToAttributeInfo(newItem)
            storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        } else {
            val itemsList = buildItem(keys, columnName, av.value()).map { (k, v) ->
                AttributeInfo(
                    k,
                    v.toAttributeTypeInfo()
                )
            }
            storage.putItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        }
    }

    private fun deleteInUpdate(
        tableName: String, partitionKey: Key, sortKey: Key?,
        columnName: String, av: AttributeValueUpdate, keys: Map<String, AttributeValue>
    ) {
        val previousItem = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
            it.name to it.type.toAttributeValue()
        } ?: throw dynamoException("One of the required keys was not given a value")
        if (av.value() == null) {
            val newItem = previousItem.toMutableMap()
            newItem.remove(columnName)
            val itemsList = itemToAttributeInfo(newItem)
            storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        } else {
            val previousItemAttribute = previousItem[columnName]
            if (previousItemAttribute != null) {
                val setType = checkAttributes(
                    "DELETE",
                    listOf(DynamoType.NS, DynamoType.SS, DynamoType.BS),
                    previousItemAttribute,
                    av.value(),
                )
                val newItem = previousItem.toMutableMap()
                val diffSet = when (setType) {
                    DynamoType.SS -> {
                        dynamoRequires(av.value().ss().isNotEmpty()) {
                            "One or more parameter values were invalid: An string set may not be empty"
                        }
                        val diffSet = previousItemAttribute.ss() - av.value().ss()
                        newItem[columnName] = AttributeValue.builder()
                            .ss(diffSet)
                            .build()
                        diffSet
                    }
                    DynamoType.NS -> {
                        dynamoRequires(av.value().ns().isNotEmpty()) {
                            "One or more parameter values were invalid: An number set may not be empty"
                        }
                        val diffSet = previousItemAttribute.ns() - av.value().ns()
                        newItem[columnName] = AttributeValue.builder()
                            .ns(diffSet)
                            .build()
                        diffSet
                    }
                    else -> {
                        dynamoRequires(av.value().bs().isNotEmpty()) {
                            "One or more parameter values were invalid: An binary set may not be empty"
                        }
                        val diffSet = previousItemAttribute.bs() - av.value().bs()
                        newItem[columnName] = AttributeValue.builder()
                            .bs(diffSet)
                            .build()
                        diffSet
                    }
                }

                if (diffSet.isEmpty()) {
                    deleteItem(
                        DeleteItemRequest.builder()
                            .key(keys)
                            .tableName(tableName)
                            .build()
                    )
                } else {
                    val itemsList = itemToAttributeInfo(newItem)
                    storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
                }
            }
        }
    }

    private fun addInUpdate(
        tableName: String, partitionKey: Key, sortKey: Key?,
        columnName: String, av: AttributeValueUpdate, keys: Map<String, AttributeValue>
    ) {
        val previousItem = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
            it.name to it.type.toAttributeValue()
        }
        if (previousItem == null) {
            checkAttributes("ADD", listOf(DynamoType.N, DynamoType.NS), av.value())
            val itemsList = itemToAttributeInfo(buildItem(keys, columnName, av.value()))
            storage.putItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        } else {
            val previousItemAttribute = previousItem[columnName]
            if (previousItemAttribute != null) {
                val attrType = checkAttributes(
                    "ADD",
                    listOf(DynamoType.N, DynamoType.NS, DynamoType.SS, DynamoType.BS),
                    previousItemAttribute,
                    av.value()
                )
                val newItem = previousItem.toMutableMap()
                when (attrType) {
                    DynamoType.SS -> {
                        val newSet = previousItemAttribute.ss() + av.value().ss()
                        newItem[columnName] = AttributeValue.builder()
                            .ss(newSet)
                            .build()
                    }
                    DynamoType.NS -> {
                        val newSet = previousItemAttribute.ns() + av.value().ns()
                        newItem[columnName] = AttributeValue.builder()
                            .ns(newSet)
                            .build()
                    }
                    DynamoType.BS -> {
                        val newSet = previousItemAttribute.bs() + av.value().bs()
                        newItem[columnName] = AttributeValue.builder()
                            .bs(newSet)
                            .build()
                    }
                    else -> {
                        // number
                        val newNum = previousItemAttribute.n().toBigDecimal() + av.value().n().toBigDecimal()
                        newItem[columnName] = AttributeValue.builder()
                            .n(newNum.toString())
                            .build()
                    }
                }
                val itemsList = itemToAttributeInfo(newItem)
                storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
            } else {
                checkAttributes(
                    "ADD",
                    listOf(DynamoType.N, DynamoType.NS, DynamoType.SS, DynamoType.BS),
                    av.value()
                )
                val newItem = previousItem.toMutableMap()
                newItem[columnName] = av.value()
                val itemsList = itemToAttributeInfo(newItem)
                storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
            }
        }
    }

    fun updateItem(request: UpdateItemRequest): UpdateItemResponse {
        val tableName = request.tableName()
        val key = request.key()
        checkNumOfKeys(tableName, key)
        val (partitionKey, sortKey) = getRequestMetadata(tableName, key)

        val lsi = checkTableExists(tableName).localSecondaryIndexes

        val attributeValues = request.attributeUpdates()
        attributeValues.forEach { (columnName, av) ->
            if (key.containsKey(columnName)) {
                throw dynamoException(
                    "One or more parameter values were invalid: Cannot update attribute ${columnName}." +
                        "This attribute is part of the key"
                )
            }

            if (lsi.containsKey(columnName)) {
                // todo: check the message in dynamo
                throw dynamoException(
                    "One or more parameter values were invalid: Cannot update attribute ${columnName}." +
                        "This attribute is part of the locan secondary index"
                )
            }
            if (av.action() == AttributeAction.PUT) {
                putInUpdate(tableName, partitionKey, sortKey, columnName, av, key)
            } else if (av.action() == AttributeAction.DELETE) {
                deleteInUpdate(tableName, partitionKey, sortKey, columnName, av, key)
            } else if (av.action() == AttributeAction.ADD) {
                addInUpdate(tableName, partitionKey, sortKey, columnName, av, key)
            }

        }

        return UpdateItemResponse.builder()
            .build()
    }

    fun batchWriteItem(request: BatchWriteItemRequest): BatchWriteItemResponse {
        dynamoRequires(request.hasRequestItems() && request.requestItems().isNotEmpty()) {
            "BatchWriteItem cannot have a null or no requests set"
        }
        val requestItems = request.requestItems()
        val putItemRequests = mutableListOf<DBPutItemRequest>()
        val deleteItemRequests = mutableListOf<DBDeleteItemRequest>()

        requestItems.keys.forEach {
            checkTableExists(it)
        }

        val batchSize = requestItems.entries.sumOf { it.value.size }

        dynamoRequires(batchSize <= 25) {
            "Too many items requested for the BatchWriteItem call"
        }

        requestItems.entries.forEach { tableRequests ->
            val items = mutableSetOf<Map<String, AttributeValue>>()
            val tableName = tableRequests.key
            tableRequests.value.forEach {
                val putRequest = it.putRequest()
                val deleteRequest = it.deleteRequest()
                if (putRequest == null && deleteRequest == null) {
                    throw dynamoException(
                        "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"
                    )
                } else if (putRequest != null && putRequest.hasItem() && (deleteRequest == null || !deleteRequest.hasKey())) {
                    val (partitionKey, sortKey) = getRequestMetadata(tableName, putRequest.item())
                    val keys = putRequest.item().filter { item ->
                        item.key == partitionKey.attributeName || item.key == sortKey?.attributeName
                    }
                    dynamoRequires(keys !in items) {
                        "Provided list of item keys contains duplicates"
                    }

                    items.add(keys)
                    val itemsList = itemToAttributeInfo(putRequest.item())
                    putItemRequests.add(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
                } else if (deleteRequest != null && deleteRequest.hasKey() && (putRequest == null || !putRequest.hasItem())) {
                    val (partitionKey, sortKey) = getRequestMetadata(tableName, deleteRequest.key())

                    val keys = deleteRequest.key()
                    dynamoRequires(keys !in items) {
                        "Provided list of item keys contains duplicates"
                    }

                    items.add(keys)
                    deleteItemRequests.add(DBDeleteItemRequest(tableName, partitionKey, sortKey))
                } else {
                    throw dynamoException(
                        "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"
                    )
                }
            }

        }

        putItemRequests.forEach {
            val previousItem =
                storage.getItem(DBGetItemRequest(it.tableName, it.partitionKey, it.sortKey))?.associate { v ->
                    v.name to v.type.toAttributeValue()
                }

            if (previousItem != null) {
                storage.updateItem(it)
            } else {
                storage.putItem(it)
            }
        }
        deleteItemRequests.forEach { storage.deleteItem(it) }

        return BatchWriteItemResponse.builder()
            .build()
    }
}