package ru.hse.dynamomock.db


import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse

interface DataStorageLayer {
    fun createTable(tableMetadata: TableMetadata)

    fun putItem(request: HSQLDBPutItemRequest)

    fun getItem(request: HSQLDBGetItemRequest): GetItemResponse

    // TODO support other queries
}