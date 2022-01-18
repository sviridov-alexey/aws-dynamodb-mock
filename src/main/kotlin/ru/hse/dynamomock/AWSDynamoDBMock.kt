@file:Suppress("unused")

package ru.hse.dynamomock

import ru.hse.dynamomock.db.ExposedStorage
import ru.hse.dynamomock.service.AWSDynamoDBMockService
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.concurrent.CompletableFuture
import java.io.InputStream

class AWSDynamoDBMock : DynamoDbClient {
    private val service by lazy { AWSDynamoDBMockService(ExposedStorage()) }

    override fun close() = service.close()

    override fun serviceName(): String = SERVICE_NAME

    override fun createTable(createTableRequest: CreateTableRequest): CreateTableResponse {
        val description = service.createTable(createTableRequest)
        return CreateTableResponse.builder()
            .tableDescription(description)
            .build()
    }

    override fun deleteTable(deleteTableRequest: DeleteTableRequest): DeleteTableResponse {
        val description = service.deleteTable(deleteTableRequest)
        return DeleteTableResponse.builder()
            .tableDescription(description)
            .build()
    }

    override fun describeTable(describeTableRequest: DescribeTableRequest): DescribeTableResponse {
        val description = service.describeTable(describeTableRequest)
        return DescribeTableResponse.builder()
            .table(description)
            .build()
    }

    override fun query(queryRequest: QueryRequest) = service.query(queryRequest)

    override fun scan(scanRequest: ScanRequest) = service.scan(scanRequest)

    override fun putItem(putItemRequest: PutItemRequest): PutItemResponse {
        return service.putItem(putItemRequest)
    }

    override fun getItem(getItemRequest: GetItemRequest): GetItemResponse {
        return service.getItem(getItemRequest)
    }

    override fun deleteItem(deleteItemRequest: DeleteItemRequest): DeleteItemResponse {
        return service.deleteItem(deleteItemRequest)
    }

    override fun batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): BatchWriteItemResponse {
        return service.batchWriteItem(batchWriteItemRequest)
    }

    fun loadCSV(fileName: String, tableName: String) {
        service.loadCSV(fileName, tableName)
    }

    fun loadCSV(inputStream: InputStream, tableName: String) {
        service.loadCSV(inputStream, tableName)
    }

    companion object {
        private const val SERVICE_NAME = "dynamodb"
    }
}

class AWSDynamoDBAsyncMock : DynamoDbAsyncClient {
    private val mock = AWSDynamoDBMock()

    override fun close() = mock.close()

    override fun serviceName(): String = mock.serviceName()

    override fun createTable(
        createTableRequest: CreateTableRequest
    ): CompletableFuture<CreateTableResponse> = CompletableFuture.supplyAsync {
        mock.createTable(createTableRequest)
    }

    override fun deleteTable(
        deleteTableRequest: DeleteTableRequest
    ): CompletableFuture<DeleteTableResponse> = CompletableFuture.supplyAsync {
        mock.deleteTable(deleteTableRequest)
    }

    override fun describeTable(
        describeTableRequest: DescribeTableRequest
    ): CompletableFuture<DescribeTableResponse> = CompletableFuture.supplyAsync {
        mock.describeTable(describeTableRequest)
    }

    override fun query(
        queryRequest: QueryRequest
    ): CompletableFuture<QueryResponse> = CompletableFuture.supplyAsync {
        mock.query(queryRequest)
    }

    override fun putItem(
        putItemRequest: PutItemRequest
    ): CompletableFuture<PutItemResponse> = CompletableFuture.supplyAsync {
        mock.putItem(putItemRequest)
    }

    override fun getItem(
        getItemRequest: GetItemRequest
    ): CompletableFuture<GetItemResponse> = CompletableFuture.supplyAsync {
        mock.getItem(getItemRequest)
    }

    override fun deleteItem(
        deleteItemRequest: DeleteItemRequest
    ): CompletableFuture<DeleteItemResponse> = CompletableFuture.supplyAsync {
        mock.deleteItem(deleteItemRequest)
    }

    override fun batchWriteItem(
        batchWriteItemRequest: BatchWriteItemRequest
    ): CompletableFuture<BatchWriteItemResponse> = CompletableFuture.supplyAsync {
        mock.batchWriteItem(batchWriteItemRequest)
    }

    fun loadCSV(filename: String, tableName: String) = mock.loadCSV(filename, tableName)

    fun loadCSV(inputStream: InputStream, tableName: String) = mock.loadCSV(inputStream, tableName)
}
