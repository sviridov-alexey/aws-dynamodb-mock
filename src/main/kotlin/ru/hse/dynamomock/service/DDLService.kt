package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.toTableMetadata
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.TableDescription

class DDLService(
    private val storage: DataStorageLayer,
    private val tablesMetadata: MutableMap<String, TableMetadata>
) {

    fun createTable(request: CreateTableRequest): TableDescription {
        dynamoRequires(request.tableName() !in tablesMetadata) {
            "Table ${request.tableName()} already exists. Cannot create."
        }
        return request.toTableMetadata().also {
            storage.createTable(it)
            tablesMetadata[it.tableName] = it
        }.toTableDescription()
    }

    fun deleteTable(request: DeleteTableRequest): TableDescription {
        val name = request.tableName()
        dynamoRequires(name in tablesMetadata) {
            "Cannot delete non-existent table."
        }
        storage.deleteTable(name)
        return tablesMetadata.remove(name)!!.toTableDescription()
    }

    fun describeTable(request: DescribeTableRequest): TableDescription {
        val name = request.tableName()
        dynamoRequires(name in tablesMetadata) {
            "Cannot describe non-existent table $name."
        }
        return tablesMetadata.getValue(name).toTableDescription()
    }

}