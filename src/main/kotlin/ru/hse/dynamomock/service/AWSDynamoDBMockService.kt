package ru.hse.dynamomock.service

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.exception.AWSMockCSVException
import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.query.*
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.*

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val tablesMetadata = mutableMapOf<String, TableMetadata>()

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

    fun query(request: QueryRequest): QueryResponse = request.toSelectRequest().select().toQueryResponse()

    fun scan(request: ScanRequest): ScanResponse = request.toSelectRequest().select().toScanResponse()

    fun putItem(request: PutItemRequest): PutItemResponse {
        val tableName = request.tableName()

        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.item())

        val itemsList = request.item().map { (k, v) -> AttributeInfo(k, v.toAttributeTypeInfo()) }

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
            val value = it.split("|").map { v -> v.trim() }
            if (value.size != 2) {
                throw AWSMockCSVException("Wrong value format. Use <column_name>|<type>")
            }

            val (columnName, type) = value.zipWithNext().firstOrNull()
                ?: throw AWSMockCSVException("Wrong value format. Use <column_name>|")
            if (type !in allowedTypes) {
                throw AWSMockCSVException("Function loadItems supports all types except B, BS")
            }
            header.add(columnName to type)
        }

        rows.forEach {
            val item = it.mapIndexed { index, element ->
                val (columnName, type) = header[index]
                columnName to toAttributeValue(type, element)
            }.toMap()

            putItem(
                PutItemRequest.builder()
                    .item(item)
                    .tableName(tableName)
                    .build()
            )
        }
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

    // TODO take into account that it's impossible to use expression-like and not-expression-like at the same query
    private fun SelectRequest.select(): SelectResponseBuilder {
        dynamoRequires(limit == null || limit > 0) {
            "Limit must be >= 1."
        }
        dynamoRequires(consistentRead != false) {
            "Non-consistent read is not supported in query yet."
        }

        dynamoRequires(tableName in tablesMetadata) {
            "Cannot query from non-existent table $tableName."
        }

        val table = tablesMetadata.getValue(tableName)
        val index = indexName?.let { table.localSecondaryIndex(it) }
        val keyConditions = retrieveKeyConditions()?.also { keyConditions ->
            keyConditions[table.partitionKey].let {
                dynamoRequires(it != null && it.comparisonOperator() == ComparisonOperator.EQ) {
                    "Partition key must use '=' operator."
                }
            }
            dynamoRequires(keyConditions.size <= 2) {
                "Length of key conditions must be 1 or 2."
            }
            if (index == null) {
                dynamoRequires(keyConditions.size == 1 || table.sortKey in keyConditions) {
                    "Only partition and sort keys can be used in key conditions."
                }
            } else {
                dynamoRequires(keyConditions.size == 1 || index.sortKey in keyConditions) {
                    "Only partition key and index sort key can be used in key conditions."
                }
            }
        }

        val items = storage.query(tableName, keyConditions).let {
            if (scanIndexForward) it else it.reversed()
        }.map { item ->
            item.associate { it.name to it.type.toAttributeValue() }
        }.let { items ->
            exclusiveStartKey?.let { exclusiveStartKey ->
                val sortKey = exclusiveStartKey[table.sortKey]
                items.asSequence()
                    .dropWhile { it[table.sortKey] != sortKey }
                    .drop(1)
                    .toList()
            } ?: items
        }.let { items ->
            limit?.let { items.take(it) } ?: items
        }

        val filterExpression = retrieveFilterExpression()
        val filteredItems = filterExpression?.let {
            items.filter {
                val withoutKeys = it.toMutableMap().apply {
                    remove(table.partitionKey)
                    remove(table.sortKey)
                }
                filterExpression.evaluate(withoutKeys)
            }
        } ?: items

        val responseBuilder = SelectResponseBuilder().apply {
            count = filteredItems.size
            scannedCount = items.size
        }

        if (items.size == limit) {
            responseBuilder.lastEvaluatedKey = items.last()
        } else {
            responseBuilder.lastEvaluatedKey = DefaultSdkAutoConstructMap.getInstance()
        }

        val transformer = retrieveAttributesTransformer(table)
        if (select == Select.COUNT) {
            responseBuilder.items = DefaultSdkAutoConstructList.getInstance()
        } else {
            responseBuilder.items = filteredItems.map(transformer)
        }
        return responseBuilder
    }
}