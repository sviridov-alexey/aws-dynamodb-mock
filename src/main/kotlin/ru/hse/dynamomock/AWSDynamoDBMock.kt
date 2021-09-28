@file:Suppress("unused")

package ru.hse.dynamomock

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer


class AWSDynamoDBMock : DynamoDbClient {
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
        TODO("Not yet implemented")
    }

    override fun putItem(putItemRequest: Consumer<PutItemRequest.Builder>): PutItemResponse {
        TODO("Not yet implemented")
    }

    override fun putItem(putItemRequest: PutItemRequest): PutItemResponse {
        TODO("Not yet implemented")
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