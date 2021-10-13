package ru.hse.dynamomock.db


import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBGetItemResponse
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata

interface DataStorageLayer {
    fun createTable(tableMetadata: TableMetadata)

    fun putItem(request: HSQLDBPutItemRequest)

    fun getItem(request: HSQLDBGetItemRequest): HSQLDBGetItemResponse?

    // TODO support other queries
}