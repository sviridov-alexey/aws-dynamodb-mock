@file:Suppress("unused")

package ru.hse.dynamomock

import ru.hse.dynamomock.db.ExposedStorage
import ru.hse.dynamomock.service.AWSDynamoDBMockService
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

class AWSDynamoDBMock : DynamoDbClient {
    private val service by lazy { AWSDynamoDBMockService(ExposedStorage()) }

    override fun close() {
        TODO("Not yet implemented")
    }

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

    companion object {
        private const val SERVICE_NAME = "dynamodb"
    }
}
