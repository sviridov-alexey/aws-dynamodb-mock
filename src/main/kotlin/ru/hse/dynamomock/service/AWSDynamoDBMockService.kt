package ru.hse.dynamomock.service

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import ru.hse.dynamomock.exception.AWSMockCSVException
import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.ImportableType.*
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.Base64
import ru.hse.dynamomock.model.query.retrieveAttributesTransformer
import ru.hse.dynamomock.model.query.retrieveFilterExpression
import ru.hse.dynamomock.model.query.retrieveKeyConditions
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap

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
            throw dynamoException("Member must satisfy enum value set: [B, N, S]")
        }

    fun createTable(request: CreateTableRequest): TableDescription {
        dynamoRequires(request.tableName() !in tablesMetadata) {
            "Table ${request.tableName()} already exists. Cannot create."
        }
        return request.toTableMetadata().also {
            storage.createTable(it)
            tablesMetadata[it.tableName] = it
        }.toTableDescription()
    }

    fun deleteTable(request: DeleteTableRequest): TableDescription {
        val name = request.tableName()
        dynamoRequires(name in tablesMetadata) {
            "Cannot delete non-existent table."
        }
        storage.deleteTable(name)
        return tablesMetadata.remove(name)!!.toTableDescription()
    }

    fun describeTable(request: DescribeTableRequest): TableDescription {
        val name = request.tableName()
        dynamoRequires(name in tablesMetadata) {
            "Cannot describe non-existent table $name."
        }
        return tablesMetadata.getValue(name).toTableDescription()
    }

    // TODO take into account that it's impossible to use expression-like and not-expression-like at the same query
    fun query(request: QueryRequest): QueryResponse {
        dynamoRequires(request.limit() == null || request.limit() > 0) {
            "Limit must be >= 1."
        }
        dynamoRequires(request.indexName() == null) {
            "Indexes are not supported in query yet."
        }
        dynamoRequires(request.consistentRead() != false) {
            "Non-consistent read is not supported in query yet."
        }

        val tableName = request.tableName()
        dynamoRequires(tableName in tablesMetadata) {
            "Cannot query from non-existent table $tableName."
        }

        val table = tablesMetadata.getValue(tableName)
        val keyConditions = request.retrieveKeyConditions()
        keyConditions[table.partitionKey].let {
            dynamoRequires(it != null && it.comparisonOperator() == ComparisonOperator.EQ) {
                "Partition key must use '=' operator."
            }
        }
        dynamoRequires(keyConditions.size <= 2 && !(keyConditions.size == 2 && table.sortKey !in keyConditions)) {
            "Only partition and sort keys can be used in key conditions."
        }

        val items = storage.query(tableName, keyConditions).let {
            if (request.scanIndexForward() != false) it else it.reversed()
        }.map { item ->
            item.associate { it.name to it.type.toAttributeValue() }
        }.let { items ->
            request.exclusiveStartKey().takeIf { request.hasExclusiveStartKey() }?.let { exclusiveStartKey ->
                val sortKey = exclusiveStartKey[table.sortKey]
                items.asSequence()
                    .dropWhile { it[table.sortKey] != sortKey }
                    .drop(1)
                    .toList()
            } ?: items
        }.let { items ->
            request.limit()?.let { items.take(it) } ?: items
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

        if (items.size == request.limit()) {
            responseBuilder.lastEvaluatedKey(items.last())
        } else {
            responseBuilder.lastEvaluatedKey(DefaultSdkAutoConstructMap.getInstance())
        }

        val transformer = request.retrieveAttributesTransformer()
        if (request.select() != Select.COUNT) {
            responseBuilder.items(filteredItems.map(transformer))
        } else {
            responseBuilder.items(DefaultSdkAutoConstructMap.getInstance())
        }
        return responseBuilder.build()
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
        dynamoRequires(batchWriteItemRequest.hasRequestItems() && batchWriteItemRequest.requestItems().isNotEmpty()) {
            "BatchWriteItem cannot have a null or no requests set"
        }
        val requestItems = batchWriteItemRequest.requestItems()
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
                    val itemsList = putRequest.item().map { (k, v) -> AttributeInfo(k, v.toAttributeTypeInfo()) }
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
        deleteItemRequests.forEach { storage.deleteItem(it) }

        return BatchWriteItemResponse.builder()
            .build()
    }

    fun loadCSV(fileName: String, tableName: String) {
        val allowedTypes = ImportableType.values().map { e -> e.name }
        val header = mutableListOf<Pair<String, String>>()
        val rows = mutableListOf<List<String>>()
        csvReader {
            delimiter = ';'
            quoteChar = '"'
        }.open(fileName) {
            readAllAsSequence().forEach { row ->
                rows.add(row)
            }
        }
        val firstRow = rows.removeFirstOrNull() ?: throw AWSMockCSVException("The file is empty")
        firstRow.forEach {
            val value = it.split("|").map{ v -> v.trim() }
            if (value.size != 2) {
                throw AWSMockCSVException("Wrong value format. Use <column_name>|<type>")
            }

            val (columnName, type) = value.zipWithNext().firstOrNull() ?: throw AWSMockCSVException("Wrong value format. Use <column_name>|")
            if (type !in allowedTypes) {
                throw AWSMockCSVException("Function scanItems supports only S, N, NS, SS, NULL, BOOL types right now")
            }
            header.add(columnName to type)
        }

        rows.forEach {
            val item = it.mapIndexed { index, element ->
                val (columnName, type) = header[index]
                columnName to when (ImportableType.valueOf(type)) {
                    S -> AttributeValue.builder().s(element).build()
                    N -> AttributeValue.builder().n(element).build()
                    NS -> {
                        val list = element.split(",")
                        AttributeValue.builder().ns(list).build()
                    }
                    SS -> {
                        val list = element.split(",")
                        AttributeValue.builder().ss(list).build()
                    }
                    NULL -> {
                        AttributeValue.builder().nul(element == "true").build()
                    }
                    BOOL -> {
                        AttributeValue.builder().bool(element == "true").build()
                    }
                    L -> {
                        val attributeValues = Json.decodeFromString<List<AttributeTypeInfo>>(element)
                        AttributeValue.builder()
                            .l(attributeValues.map { v -> v.toAttributeValue() })
                            .build()
                    }
                    M -> {
                        val attributeValues = Json.decodeFromString<Map<String, AttributeTypeInfo>>(element)
                        AttributeValue.builder().m(
                            attributeValues.map { v -> v.key to v.value.toAttributeValue() }.toMap()
                        ).build()
                    }
                }
            }.toMap()

            putItem(
                PutItemRequest.builder()
                    .item(item)
                    .tableName(tableName)
                    .build()
            )
        }
    }

    private fun getKeyFromMetadata(keyName: String, keys: Map<String, AttributeValue>, attributeDefinitions: List<AttributeDefinition>): Key {
        val keyAttributeValue = keys[keyName] ?: throw dynamoException("One of the required keys was not given a value")
        val (keyType, key) = toKey(keyName, keyAttributeValue)
        val expectedKeyType = attributeDefinitions.firstOrNull { it.attributeName() == keyName }
            ?: throw dynamoException("One of the required keys was not given a value")
        dynamoRequires(expectedKeyType.attributeTypeAsString().uppercase() == keyType) {
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
        request: DBGetItemRequest
    ): Map<String, AttributeValue>? =
        when (returnValues) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(request)?.associate {
                    it.name to it.type.toAttributeValue()
                }
            }
            ReturnValue.NONE, null -> null
            else -> throw dynamoException("Return values set to invalid value")
        }

    private fun checkNumOfKeys(tableName: String, keys: Map<String, AttributeValue>) {
        val tableMetadata = tablesMetadata[tableName] ?: throw ResourceNotFoundException.builder()
            .message("Cannot do operations on a non-existent table").build()
        val actualSize = if (tableMetadata.sortKey != null) 2 else 1
        if (keys.size > actualSize) {
            throw DynamoDbException.builder()
                .message("The number of conditions on the keys is invalid")
                .build()
        }
    }
}