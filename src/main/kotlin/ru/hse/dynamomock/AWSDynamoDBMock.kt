@file:Suppress("unused")

package ru.hse.dynamomock

import ru.hse.dynamomock.db.ExposedStorage
import ru.hse.dynamomock.model.toTableDescription
import ru.hse.dynamomock.service.AWSDynamoDBMockService
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

class AWSDynamoDBMock : DynamoDbClient {
    private val service by lazy { AWSDynamoDBMockService(ExposedStorage(DATABASE_NAME)) }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun serviceName(): String = SERVICE_NAME

    override fun createTable(createTableRequest: CreateTableRequest): CreateTableResponse {
        val description = service.createTable(createTableRequest).toTableDescription()
        return CreateTableResponse.builder()
            .tableDescription(description)
            .build()
    }

    override fun putItem(putItemRequest: PutItemRequest): PutItemResponse {
        service.putItem(putItemRequest)
        return PutItemResponse.builder().build()
    }

    override fun getItem(getItemRequest: GetItemRequest): GetItemResponse {
        return service.getItem(getItemRequest)
    }

    companion object {
        private const val SERVICE_NAME = "dynamodb"
        private const val DATABASE_NAME = "testDB"
    }
}
