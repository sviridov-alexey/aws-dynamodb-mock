package ru.hse.dynamomock.db


import ru.hse.dynamomock.model.*
import software.amazon.awssdk.services.dynamodb.model.Condition

interface DataStorageLayer {
    fun createTable(tableMetadata: TableMetadata)

    fun deleteTable(tableName: String)

    fun putItem(request: DBPutItemRequest)

    fun getItem(request: DBGetItemRequest): List<AttributeInfo>?

    fun deleteItem(request: DBDeleteItemRequest)

    fun updateItem(request: DBPutItemRequest)

    fun query(tableName: String, keyConditions: Map<String, Condition>): List<List<AttributeInfo>>

    // TODO support other queries
}