package ru.hse.dynamomock.service

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.exception.AWSMockCSVException
import ru.hse.dynamomock.model.*
import software.amazon.awssdk.services.dynamodb.model.*
import java.io.File
import java.io.InputStream

class AWSDynamoDBMockService(
    private val storage: DataStorageLayer,
    private val ddlService: DDLService,
    private val dmlService: DMLService,
    private val selectService: SelectService
) {

    fun close() = storage.close()

    fun createTable(request: CreateTableRequest): TableDescription = ddlService.createTable(request)

    fun deleteTable(request: DeleteTableRequest): TableDescription = ddlService.deleteTable(request)

    fun describeTable(request: DescribeTableRequest): TableDescription = ddlService.describeTable(request)

    fun query(request: QueryRequest): QueryResponse = selectService.query(request)

    fun scan(request: ScanRequest): ScanResponse = selectService.scan(request)

    fun putItem(request: PutItemRequest): PutItemResponse = dmlService.putItem(request)

    fun getItem(request: GetItemRequest): GetItemResponse = dmlService.getItem(request)

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse = dmlService.deleteItem(request)

    fun updateItem(request: UpdateItemRequest): UpdateItemResponse = dmlService.updateItem(request)

    fun batchWriteItem(request: BatchWriteItemRequest): BatchWriteItemResponse = dmlService.batchWriteItem(request)

    fun loadCSV(filePath: String, tableName: String) {
        loadCSV(File(filePath).inputStream(), tableName)
    }

    fun loadCSV(inputStream: InputStream, tableName: String) {
        val rows = csvReader {
            delimiter = ';'
            quoteChar = '"'
        }.readAll(inputStream).toMutableList()
        processCSV(rows, tableName)
    }

    private fun processCSV(rows: MutableList<List<String>>, tableName: String) {
        val allowedTypes = ImportableType.values().map { e -> e.name }
        val header = mutableListOf<Pair<String, String>>()

        val firstRow = rows.removeFirstOrNull() ?: throw AWSMockCSVException("The file is empty")
        firstRow.forEach {
            val value = it.split("|").map { v -> v.trim() }
            if (value.size != 2) {
                throw AWSMockCSVException("Wrong value format. Use <column_name>|<type>")
            }

            val (columnName, type) = value.zipWithNext().firstOrNull()
                ?: throw AWSMockCSVException("Wrong value format. Use <column_name>|")
            if (type !in allowedTypes) {
                throw AWSMockCSVException("Function loadItems supports all types except B, BS")
            }
            header.add(columnName to type)
        }

        rows.forEach {
            val item = it.mapIndexed { index, element ->
                val (columnName, type) = header[index]
                columnName to toAttributeValue(type, element)
            }.toMap()

            putItem(
                PutItemRequest.builder()
                    .item(item)
                    .tableName(tableName)
                    .build()
            )
        }
    }

}