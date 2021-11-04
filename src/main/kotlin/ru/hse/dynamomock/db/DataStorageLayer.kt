package ru.hse.dynamomock.db


import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.DBDeleteItemRequest
import ru.hse.dynamomock.model.DBGetItemRequest
import ru.hse.dynamomock.model.DBPutItemRequest
import ru.hse.dynamomock.model.DBUpdateItemRequest
import ru.hse.dynamomock.model.TableMetadata

interface DataStorageLayer {
    fun createTable(tableMetadata: TableMetadata)

    fun deleteTable(tableName: String)

    fun putItem(request: DBPutItemRequest)

    fun getItem(request: DBGetItemRequest): List<AttributeInfo>?

    fun deleteItem(request: DBDeleteItemRequest)

    fun updateItem(request: DBUpdateItemRequest)

    // TODO support other queries
}