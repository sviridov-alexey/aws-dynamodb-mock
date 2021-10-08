package ru.hse.dynamomock.db

import ru.hse.dynamomock.db.util.AttributeInfo
import ru.hse.dynamomock.db.util.SqlQuerier
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Suppress("unused")
class HSQLDBStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
): DataStorageLayer, AutoCloseable {
    private val connection: Connection = connect(dbname, username, password)

    override fun close() = connection.close()

    override fun createTable(request: CreateTableRequest): TableMetadata {
        val tableMetadata = TableMetadata(
            request.tableName(),
            request.attributeDefinitions(),
            request.keySchema(),
            TableStatus.ACTIVE
        )
        val createTableQuery = SqlQuerier.createTableQuery(tableMetadata)
        connection.prepareStatement(createTableQuery).use { it.execute() }

        return tableMetadata
    }

    private fun convertAttributeValueToInfo(attributeName: String, attributeValue: AttributeValue): AttributeInfo {
        var type = ""
        var item: Any? = null
        if (attributeValue.bool() != null) {
            type = "Boolean"
            item = attributeValue.bool()
        } else if (attributeValue.s() != null) {
            type = "String"
            item = attributeValue.s()
        }
        // TODO: other types

        return AttributeInfo(attributeName, type, item)
    }

    override fun putItem(request: PutItemRequest) {
        val tableName = request.tableName()
        val itemsList = mutableListOf<AttributeInfo>()
        request.item().forEach{
            itemsList.add(convertAttributeValueToInfo(it.key, it.value))
        }
        val putItemQuery = SqlQuerier.putItemQuery(tableName, itemsList)
        connection.prepareStatement(putItemQuery).use { it.execute() }
    }

    override fun getItem(request: GetItemRequest, description: TableDescription): GetItemResponse {
        val tableName = request.tableName()
        val keys = request.key()
        if (keys.size > 1) {
            // TODO: sort key
        }

        var partitionKey: AttributeInfo = AttributeInfo("", "", "")
        keys.toList().stream().findAny().ifPresent {
            partitionKey = convertAttributeValueToInfo(it.first, it.second)
        }

        val getItemQuery = SqlQuerier.getItemQuery(tableName, partitionKey, request.attributesToGet())
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