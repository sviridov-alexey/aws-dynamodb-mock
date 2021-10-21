package ru.hse.dynamomock.db


import ru.hse.dynamomock.model.DBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBGetItemResponse
import ru.hse.dynamomock.model.DBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata

interface DataStorageLayer {
    fun createTable(tableMetadata: TableMetadata)

    fun putItem(request: DBPutItemRequest)

    fun getItem(request: DBGetItemRequest): HSQLDBGetItemResponse?

    // TODO support other queries
}