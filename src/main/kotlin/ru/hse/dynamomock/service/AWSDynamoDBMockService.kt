package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.model.*
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.util.Base64

class AWSDynamoDBMockService(private val storage: DataStorageLayer) {
    private val tablesMetadata = mutableMapOf<String, TableMetadata>()

    private fun toAttributeTypeInfo(value: AttributeValue) : AttributeTypeInfo =
        AttributeTypeInfo(
            s = value.s(),
            n = value.n(),
            b = value.b()?.let { Base64.getEncoder().encodeToString(value.b().asByteArray()) },
            ss = if (value.hasSs()) value.ss() else null,
            ns = if (value.hasNs()) value.ns() else null,
            bs = if (value.hasBs()) value.bs().map {  Base64.getEncoder().encodeToString(it.asByteArray()) } else null,
            m = if (value.hasM()) value.m().mapValues { toAttributeTypeInfo(it.value) } else null,
            l = if (value.hasL()) value.l()?.map { toAttributeTypeInfo(it) } else null,
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
            throw IllegalArgumentException("Key supports only S, N and B types!")
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

        val attributes = when (request.returnValues()) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
                    it.name to toAttributeValue(it.type)
                }
            }
            ReturnValue.NONE, null -> null
            else -> throw DynamoDbException.builder().message("Return values set to invalid value").build()
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

        val attributes = when (request.returnValues()) {
            ReturnValue.ALL_OLD -> {
                storage.getItem(DBGetItemRequest(tableName, partitionKey, sortKey))?.associate {
                    it.name to toAttributeValue(it.type)
                }
            }
            ReturnValue.NONE, null -> null
            else -> throw DynamoDbException.builder().message("Return values set to invalid value").build()
        }

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
        val tableMetadata = tablesMetadata.getValue(tableName)

        val partitionKeyName = checkNotNull(tableMetadata.partitionKey)
        return getKeyFromMetadata(partitionKeyName, keys) to getSortKeyFromMetadata(tableMetadata.sortKey, keys)
    }
}