package ru.hse.dynamomock.db

import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.TableDescription

interface DataStorageLayer {
    fun createTable(request: CreateTableRequest): TableMetadata

    fun putItem(request: PutItemRequest)

    fun getItem(request: GetItemRequest, description: TableDescription): GetItemResponse

    // TODO support other queries
}