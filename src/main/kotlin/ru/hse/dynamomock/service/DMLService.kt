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
import ru.hse.dynamomock.model.NumKey
import ru.hse.dynamomock.model.StringKey
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.toAttributeTypeInfo
import software.amazon.awssdk.services.dynamodb.model.AttributeAction
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
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
import java.util.Base64

class DMLService(
    private val storage: DataStorageLayer,
    private val tablesMetadata: MutableMap<String, TableMetadata>
) {
    private fun toKey(keyName: String, attributeValue: AttributeValue): Pair<DynamoType, Key> =
        if (attributeValue.s() != null) {
            DynamoType.S to StringKey(keyName, attributeValue.s())
        } else if (attributeValue.n() != null) {
            DynamoType.N to NumKey(keyName, attributeValue.n().toBigDecimal())
        } else if (attributeValue.b() != null) {
            DynamoType.B to StringKey(keyName, Base64.getEncoder().encodeToString(attributeValue.b().asByteArray()))
        } else {
            throw dynamoException("Member must satisfy enum value set: [B, N, S]")
        }

    private fun getKeyFromMetadata(
        keyName: String,
        keys: Map<String, AttributeValue>,
        attributeDefinitions: List<AttributeDefinition>
    ): Key {
        val keyAttributeValue = keys[keyName] ?: throw dynamoException("One of the required keys was not given a value")
        val (keyType, key) = toKey(keyName, keyAttributeValue)
        val expectedKeyType = attributeDefinitions.firstOrNull { it.attributeName() == keyName }
            ?: throw dynamoException("One of the required keys was not given a value")
        dynamoRequires(expectedKeyType.attributeTypeAsString().uppercase() == keyType.name) {
            "Invalid attribute value type"
        }
        return key
    }

    private fun getSortKeyFromMetadata(
        sortKeyName: String?,
        keys: Map<String, AttributeValue>,
        attributeDefinitions: List<AttributeDefinition>
    ): Key? {
        sortKeyName ?: return null

        return getKeyFromMetadata(sortKeyName, keys, attributeDefinitions)
    }

    private fun getRequestMetadata(tableName: String, keys: Map<String, AttributeValue>): Pair<Key, Key?> {
        val tableMetadata = tablesMetadata[tableName] ?: throw ResourceNotFoundException.builder()
            .message("Cannot do operations on a non-existent table").build()

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
        val tableMetadata = tablesMetadata[tableName] ?: throw ResourceNotFoundException.builder()
            .message("Cannot do operations on a non-existent table").build()
        val actualSize = if (tableMetadata.sortKey != null) 2 else 1
        if (keys.size > actualSize) {
            throw dynamoException("The number of conditions on the keys is invalid")
        }
    }

    private fun itemToAttributeInfo(item: Map<String, AttributeValue>) =
        item.map { (k, v) -> AttributeInfo(k, v.toAttributeTypeInfo()) }

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

    private fun checkAttributeValue(av: AttributeValue): DynamoType = when {
        av.n() != null -> DynamoType.N
        av.s() != null -> DynamoType.S
        av.b() != null -> DynamoType.B
        av.hasSs() -> DynamoType.SS
        av.hasNs() -> DynamoType.NS
        av.hasBs() -> DynamoType.BS
        av.hasL() -> DynamoType.L
        av.hasM() -> DynamoType.M
        else -> throw dynamoException("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
    }

    private fun checkAttributesForDelete(first: AttributeValue, second: AttributeValue): DynamoType {
        val firstType = checkAttributeValue(first)
        val secondType = checkAttributeValue(second)
        if (secondType !in listOf(DynamoType.NS, DynamoType.SS, DynamoType.BS)) {
            val errorString =
                "One or more parameter values were invalid: DELETE action with value is not supported for the type " +
                    when (secondType) {
                        DynamoType.N -> "NUMBER"
                        DynamoType.S -> "STRING"
                        DynamoType.B -> "BINARY"
                        DynamoType.BOOL -> "BOOLEAN"
                        DynamoType.NULL -> "NULL"
                        DynamoType.L -> "LIST"
                        DynamoType.M -> "MAP"
                        else -> ""
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
                val setType = checkAttributesForDelete(previousItemAttribute, av.value())
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

    private fun checkAttributesForAdd(first: AttributeValue, second: AttributeValue): DynamoType {
        val firstType = checkAttributeValue(first)
        val secondType = checkAttributeValue(second)
        if (secondType !in listOf(DynamoType.N, DynamoType.NS, DynamoType.SS, DynamoType.BS)) {
            val errorString =
                "One or more parameter values were invalid: ADD action with value is not supported for the type " +
                    when (secondType) {
                        DynamoType.S -> "STRING"
                        DynamoType.B -> "BINARY"
                        DynamoType.BOOL -> "BOOLEAN"
                        DynamoType.NULL -> "NULL"
                        DynamoType.L -> "LIST"
                        DynamoType.M -> "MAP"
                        else -> ""
                    }
            throw dynamoException(errorString)
        }
        if (firstType != secondType) {
            throw dynamoException("Type mismatch for attribute to update")
        }
        return secondType
    }

    private fun addInUpdate(
        tableName: String, partitionKey: Key, sortKey: Key?,
        columnName: String, av: AttributeValueUpdate, keys: Map<String, AttributeValue>
    ) {
        val previousItem = storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
            it.name to it.type.toAttributeValue()
        }
        if (previousItem == null) {
            // todo: ADD - Causes DynamoDB to create an item with the supplied primary key and number (or set of numbers)
            // for the attribute value. The only data types allowed are Number and Number Set.
            val itemsList = buildItem(keys, columnName, av.value()).map { (k, v) ->
                AttributeInfo(
                    k,
                    v.toAttributeTypeInfo()
                )
            }
            storage.putItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
        } else {
            val previousItemAttribute = previousItem[columnName]
            if (previousItemAttribute != null) {
                val attrType = checkAttributesForAdd(previousItemAttribute, av.value())
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
                checkAttributesForAdd(av.value(), av.value())
                val newItem = previousItem.toMutableMap()
                newItem[columnName] = av.value()
                val itemsList = itemToAttributeInfo(newItem)
                storage.updateItem(DBPutItemRequest(tableName, partitionKey, sortKey, itemsList))
            }
        }
    }

    fun updateItem(request: UpdateItemRequest): UpdateItemResponse {
        // todo: if attribute is key in index then check type
        val tableName = request.tableName()
        val key = request.key()
        checkNumOfKeys(tableName, key)
        val (partitionKey, sortKey) = getRequestMetadata(tableName, key)

        val attributeValues = request.attributeUpdates()
        attributeValues.forEach { (columnName, av) ->
            if (key.containsKey(columnName)) {
                throw dynamoException(
                    "One or more parameter values were invalid: Cannot update attribute ${columnName}." +
                        "This attribute is part of the key"
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
            tablesMetadata[it] ?: throw ResourceNotFoundException.builder()
                .message("Cannot do operations on a non-existent table").build()
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