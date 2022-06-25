package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import software.amazon.awssdk.services.dynamodb.model.*
import java.io.InputStream

class AWSDynamoDBMockService(
    private val storage: DataStorageLayer,
    private val ddlService: DDLService,
    private val modifyDataService: ModifyDataService,
    private val retrieveDataService: RetrieveDataService,
    private val importExportService: ImportExportService
) {

    fun close() = storage.close()

    fun createTable(request: CreateTableRequest): TableDescription = ddlService.createTable(request)

    fun deleteTable(request: DeleteTableRequest): TableDescription = ddlService.deleteTable(request)

    fun describeTable(request: DescribeTableRequest): TableDescription = ddlService.describeTable(request)

    fun query(request: QueryRequest): QueryResponse = retrieveDataService.query(request)

    fun scan(request: ScanRequest): ScanResponse = retrieveDataService.scan(request)

    fun putItem(request: PutItemRequest): PutItemResponse = modifyDataService.putItem(request)

    fun getItem(request: GetItemRequest): GetItemResponse = modifyDataService.getItem(request)

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse = modifyDataService.deleteItem(request)

    fun updateItem(request: UpdateItemRequest): UpdateItemResponse = modifyDataService.updateItem(request)

    fun batchWriteItem(request: BatchWriteItemRequest): BatchWriteItemResponse = modifyDataService.batchWriteItem(request)

    fun loadCSV(filePath: String, tableName: String) = importExportService.loadCSV(filePath, tableName)

    fun loadCSV(inputStream: InputStream, tableName: String) = importExportService.loadCSV(inputStream, tableName)

}