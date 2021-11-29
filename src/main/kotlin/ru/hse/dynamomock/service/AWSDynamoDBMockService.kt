package ru.hse.dynamomock.service

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import ru.hse.dynamomock.exception.AWSMockCSVException
import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.DynamoType.*
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.*
import java.io.File
import java.util.Base64
import java.util.EnumSet

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val tablesMetadata = mutableMapOf<String, TableMetadata>()

    private fun toAttributeTypeInfo(value: AttributeValue) : AttributeTypeInfo =
        AttributeTypeInfo(
            s = value.s(),
            n = value.n(),
            b = value.b()?.let { Base64.getEncoder().encodeToString(value.b().asByteArray()) },
            ss = value.ss().takeIf { value.hasSs() },
            ns = value.ns().takeIf { value.hasNs() },
            bs = value.bs().map {  Base64.getEncoder().encodeToString(it.asByteArray()) }.takeIf { value.hasBs() },
            m = value.m().mapValues { toAttributeTypeInfo(it.value) }.takeIf { value.hasM() },
            l = value.l()?.map { toAttributeTypeInfo(it) }.takeIf { value.hasL() },
            bool = value.bool(),
            nul = value.nul()
        )

    private fun toAttributeValue(info: AttributeTypeInfo) : AttributeValue =
        AttributeValue.builder()
            .s(info.s)
            .n(info.n)
            .b(info.b?.let { SdkBytes.fromByteArray(Base64.getDecoder().decode(info.b)) })
            .ss(info.ss)
            .ns(info.ns)
            .bs(info.bs?.map { SdkBytes.fromByteArray(Base64.getDecoder().decode(it)) })
            .m(info.m?.mapValues { toAttributeValue(it.value) })
            .l(info.l?.map { toAttributeValue(it) })
            .bool(info.bool)
            .nul(info.nul)
            .build()


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
            "Cannot describe non-existent table."
        }
        return tablesMetadata.getValue(name).toTableDescription()
    }

    fun putItem(request: PutItemRequest): PutItemResponse {
        val tableName = request.tableName()

        val (partitionKey, sortKey) = getRequestMetadata(tableName, request.item())

        val itemsList = request.item().map { (k, v) -> AttributeInfo(k, toAttributeTypeInfo(v)) }

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

        val item = response?.filter { request.attributesToGet().contains(it.name) }?.associate {
            it.name to toAttributeValue(it.type)
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
                    val itemsList = putRequest.item().map { (k, v) -> AttributeInfo(k, toAttributeTypeInfo(v)) }
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

    fun scanItems(fileName: String, tableName: String) {
        val allowedTypes = EnumSet.of(SS, S, N, NS, L, BOOL, NULL)
        val columnsInfo = mutableListOf<Pair<String, String>>()

        File(fileName).useLines { line ->
            val firstRow = line.firstOrNull() ?: throw AWSMockCSVException("The file is empty")
            val info = firstRow.split(";")
            info.forEach {
                val value = it.split("|")
                if (value.size != 2) {
                    throw AWSMockCSVException("Wrong value format. Use <column_name>|<type>")
                }
                val columnName = value[0].trim()
                val type = value[1].trim()

                if (!allowedTypes.contains(DynamoType.valueOf(type))) {
                    throw AWSMockCSVException("Function scanItems supports only S, N, NS, SS, NULL, BOOL types right now")
                }
                columnsInfo.add(columnName to type)
            }
        }

        var i = 0
        val itemsList = mutableListOf<Map<String, AttributeValue>>()
        File(fileName).forEachLine {
            if (i != 0) {
                val columnValues = it.split(";")
                if (columnValues.size != columnsInfo.size) {
                    throw AWSMockCSVException("${i+1} row's size doesn't match the size of first row")
                }
                val item = columnValues.mapIndexed { index, element ->
                    element.trim()
                    val (columnName, type) = columnsInfo[index]
                    columnName to when(DynamoType.valueOf(type)) {
                        S -> AttributeValue.builder().s(element).build()
                        N -> AttributeValue.builder().n(element).build()
                        NS -> {
                            val list = element.split(";")
                            AttributeValue.builder().ns(list).build()
                        }
                        SS -> {
                            val list = element.split(";")
                            AttributeValue.builder().ss(list).build()
                        }
                        NULL -> {
                            AttributeValue.builder().nul(element == "true").build()
                        }
                        BOOL -> {
                            AttributeValue.builder().bool(element == "true").build()
                        }
                        L -> {
                            val attributeValues = element.split(",")
                            AttributeValue.builder()
                                .l(attributeValues.map { value -> toAttributeValue(Json.decodeFromString(value)) }).build()
                        }
                        else -> throw AWSMockCSVException("Function scanItems supports only S, N, NS, SS, NULL, BOOL types right now")
                    }
                }.toMap()

                itemsList.add(item)
            }
            i++
        }
        itemsList.forEach {
            putItem(
                PutItemRequest.builder()
                    .item(it)
                    .tableName(tableName)
                    .build()
            )
        }
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
                    it.name to toAttributeValue(it.type)
                }
            }
            ReturnValue.NONE, null -> null
            else -> throw DynamoDbException.builder().message("Return values set to invalid value").build()
        }

    private fun checkNumOfKeys(tableName:String, keys: Map<String, AttributeValue>) {
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