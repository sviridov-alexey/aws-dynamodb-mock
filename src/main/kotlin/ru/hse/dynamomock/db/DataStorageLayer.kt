package ru.hse.dynamomock.db


import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.DBGetItemRequest
import ru.hse.dynamomock.model.DBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata

interface DataStorageLayer {
    fun createTable(tableMetadata: TableMetadata)

    fun putItem(request: DBPutItemRequest)

    fun getItem(request: DBGetItemRequest): List<AttributeInfo>

    // TODO support other queries
}