package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.*
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.Base64

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

    private fun toKey(keyName: String, attributeValue: AttributeValue): Key =
        if (attributeValue.s() != null) {
            StringKey(keyName, attributeValue.s())
        } else if (attributeValue.n() != null) {
            NumKey(keyName, attributeValue.n().toBigDecimal())
        } else if (attributeValue.b() != null) {
            StringKey(keyName, Base64.getEncoder().encodeToString(attributeValue.b().asByteArray()))
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

        val item = response?.filter { request.attributesToGet().contains(it.name) }?.associate {
            it.name to toAttributeValue(it.type)
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

    private fun getKeyFromMetadata(keyName: String, keys: Map<String, AttributeValue>): Key {
        val keyAttributeValue =
            keys[keyName] ?: throw DynamoDbException
                .builder()
                .message("One of the required keys was not given a value")
                .build()
        return toKey(keyName, keyAttributeValue)
    }

    private fun getSortKeyFromMetadata(sortKeyName: String?, keys: Map<String, AttributeValue>): Key? {
        sortKeyName ?: return null
        val sortKeyAttributeValue = keys[sortKeyName]
        sortKeyAttributeValue ?: return null

        return getKeyFromMetadata(sortKeyName, keys)
    }

    private fun getRequestMetadata(tableName: String, keys: Map<String, AttributeValue>): Pair<Key, Key?> {
        val tableMetadata = tablesMetadata[tableName] ?: throw ResourceNotFoundException.builder()
            .message(" Cannot do operations on a non-existent table").build()

        val partitionKeyName = tableMetadata.partitionKey
        return getKeyFromMetadata(partitionKeyName, keys) to getSortKeyFromMetadata(tableMetadata.sortKey, keys)
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

}