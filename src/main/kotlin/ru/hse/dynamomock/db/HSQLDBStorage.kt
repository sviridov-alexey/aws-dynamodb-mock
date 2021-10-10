package ru.hse.dynamomock.db

import ru.hse.dynamomock.db.util.SqlQuerier
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*
import java.sql.Connection
import java.sql.DriverManager

@Suppress("unused")
class HSQLDBStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
): DataStorageLayer, AutoCloseable {
    private val connection: Connection = connect(dbname, username, password)

    override fun close() = connection.close()

    override fun createTable(tableMetadata: TableMetadata) {
        val createTableQuery = SqlQuerier.createTableQuery(tableMetadata)
        connection.prepareStatement(createTableQuery).use { it.execute() }

    }

    override fun putItem(request: HSQLDBPutItemRequest) {
        val putItemQuery = SqlQuerier.putItemQuery(request.tableName, request.itemsList)
        connection.prepareStatement(putItemQuery).use { it.execute() }
    }

    override fun getItem(request: HSQLDBGetItemRequest): GetItemResponse {

        val getItemQuery = SqlQuerier.getItemQuery(request.tableName,
            request.partitionKey,
            request.attributesToGet)
        connection.prepareStatement(getItemQuery).use {
            val rs = it.executeQuery()
            while (rs.next()) {
                // TODO: crying in types
            }
        }

        return GetItemResponse.builder()
            .build()
    }

    companion object {
        init {
            Class.forName("org.hsqldb.jdbcDriver")
        }

        private fun connect(dbname: String, username: String, password: String) =
            DriverManager.getConnection("jdbc:hsqldb:mem:$dbname", username, password)
    }
}