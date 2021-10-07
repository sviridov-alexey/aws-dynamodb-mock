@file:Suppress("unused")

package ru.hse.dynamomock

import ru.hse.dynamomock.db.HSQLDBStorage
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer


class AWSDynamoDBMock : DynamoDbClient {
    private val dataStorageLayer by lazy { HSQLDBStorage(DATABASE_NAME) }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun serviceName(): String {
        TODO("Not yet implemented")
    }

    override fun createTable(createTableRequest: Consumer<CreateTableRequest.Builder>): CreateTableResponse {
        TODO("Not yet implemented")
    }

    override fun createTable(createTableRequest: CreateTableRequest): CreateTableResponse {
        val description = dataStorageLayer.createTable(createTableRequest).toTableDescription()
        return CreateTableResponse.builder()
            .tableDescription(description)
            .build()
    }

    override fun putItem(putItemRequest: Consumer<PutItemRequest.Builder>): PutItemResponse {
        TODO("Not yet implemented")
    }

    override fun putItem(putItemRequest: PutItemRequest): PutItemResponse {
        TODO("Not yet implemented")
    }

    companion object {
        private const val DATABASE_NAME = "testDB"
    }
}

class AWSDynamoDBAsyncMock : DynamoDbAsyncClient {
    override fun close() {
        TODO("Not yet implemented")
    }

    override fun serviceName(): String {
        TODO("Not yet implemented")
    }

    override fun createTable(createTableRequest: Consumer<CreateTableRequest.Builder>): CompletableFuture<CreateTableResponse> {
        TODO("Not yet implemented")
    }

    override fun createTable(createTableRequest: CreateTableRequest): CompletableFuture<CreateTableResponse> {
        TODO("Not yet implemented")
    }

    override fun putItem(putItemRequest: Consumer<PutItemRequest.Builder>): CompletableFuture<PutItemResponse> {
        TODO("Not yet implemented")
    }

    override fun putItem(putItemRequest: PutItemRequest): CompletableFuture<PutItemResponse> {
        TODO("Not yet implemented")
    }

}