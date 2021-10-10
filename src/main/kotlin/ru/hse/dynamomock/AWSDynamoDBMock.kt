@file:Suppress("unused")

package ru.hse.dynamomock

import ru.hse.dynamomock.db.HSQLDBStorage
import ru.hse.dynamomock.model.TableMetadata.Companion.toTableDescription
import ru.hse.dynamomock.service.HSQLDBService
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer

class AWSDynamoDBMock : DynamoDbClient {
    private val dataStorageLayer by lazy { HSQLDBStorage(DATABASE_NAME) }
    private val tableDescriptions = mutableMapOf<String, TableDescription>()
    private val service: HSQLDBService = HSQLDBService()

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun serviceName(): String = SERVICE_NAME

    override fun createTable(createTableRequest: CreateTableRequest): CreateTableResponse {
        val description: TableDescription = service.createTable(createTableRequest, dataStorageLayer).toTableDescription()
        tableDescriptions[description.tableName()] = description
        return CreateTableResponse.builder()
            .tableDescription(description)
            .build()
    }

    override fun putItem(putItemRequest: PutItemRequest): PutItemResponse {
        service.putItem(putItemRequest, dataStorageLayer)
        return PutItemResponse.builder().build()
    }

    override fun getItem(getItemRequest: GetItemRequest): GetItemResponse {
        service.getItem(getItemRequest, dataStorageLayer)
        return GetItemResponse.builder().build()
    }

    companion object {
        private const val SERVICE_NAME = "dynamodb"
        private const val DATABASE_NAME = "testDB"
    }
}

class AWSDynamoDBAsyncMock : DynamoDbAsyncClient {
    private val dynamodbMock = AWSDynamoDBMock()

    override fun close() = dynamodbMock.close()

    override fun serviceName() = dynamodbMock.serviceName()

    override fun createTable(
        createTableRequest: Consumer<CreateTableRequest.Builder>
    ): CompletableFuture<CreateTableResponse> = CompletableFuture.supplyAsync {
        dynamodbMock.createTable(createTableRequest)
    }

    override fun createTable(
        createTableRequest: CreateTableRequest
    ): CompletableFuture<CreateTableResponse> = CompletableFuture.supplyAsync {
        dynamodbMock.createTable(createTableRequest)
    }

    override fun putItem(
        putItemRequest: Consumer<PutItemRequest.Builder>
    ): CompletableFuture<PutItemResponse> = CompletableFuture.supplyAsync {
        dynamodbMock.putItem(putItemRequest)
    }

    override fun putItem(
        putItemRequest: PutItemRequest
    ): CompletableFuture<PutItemResponse> = CompletableFuture.supplyAsync {
        dynamodbMock.putItem(putItemRequest)
    }

    override fun getItem(
        getItemRequest: Consumer<GetItemRequest.Builder>
    ): CompletableFuture<GetItemResponse> = CompletableFuture.supplyAsync {
        dynamodbMock.getItem(getItemRequest)
    }

    override fun getItem(
        getItemRequest: GetItemRequest
    ): CompletableFuture<GetItemResponse> = CompletableFuture.supplyAsync {
        dynamodbMock.getItem(getItemRequest)
    }
}