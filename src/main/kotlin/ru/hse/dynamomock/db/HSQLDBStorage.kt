package ru.hse.dynamomock.db

import ru.hse.dynamomock.db.util.AttributeInfo
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
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

private object SqlQuerier {
    fun createTableQuery(tableMetadata: TableMetadata): String {
        require(tableMetadata.attributeDefinitions.isNotEmpty())
        val columns = tableMetadata.attributeDefinitions.joinToString(separator = ",") {
            "${it.attributeName()} ${TypesConverter.fromDynamoToSqlType(it.attributeType().name)}"
        }
        val primaryKey = tableMetadata.keySchema.joinToString(separator = ",") {
            it.attributeName()
        }

        //language=SQL
        return """
            CREATE TABLE ${tableMetadata.tableName} (
                $columns
                ${if (primaryKey.isNotEmpty()) ", PRIMARY KEY ($primaryKey)" else ""}
            )
        """
    }

    fun putItemQuery(tableName: String, items: List<AttributeInfo>): String {
        val columnNames = mutableListOf<String>()
        val values = mutableListOf<Any?>()
        items.forEach {
            columnNames.add(it.attributeName)
            values.add(it.attribute)
        }

        //language=SQL
        return """
            INSERT INTO $tableName (${columnNames.joinToString(", ")})
            VALUES (${values.joinToString(", ") {when (it) {
            is String -> "'$it'"
            is Boolean -> "$it"
            // TODO: is Number, but in dynamo number is string too (float?? int???)
            else -> "$it"
        }}});
        """
    }

    fun getItemQuery(tableName: String, partitionKey: AttributeInfo, attributesToGet: List<String>): String {
        //language=SQL
        return """
            SELECT ${attributesToGet.joinToString(", ")} FROM $tableName
            WHERE ${partitionKey.attributeName}=${when (partitionKey.attribute) {
            is String -> "'${partitionKey.attribute}'"
            is Boolean -> "${partitionKey.attribute}"
            // TODO: other types
            else -> "${partitionKey.attribute}"
        }};
        """
    }
}

object TypesConverter {
    fun fromDynamoToSqlType(type: String): String = when (type.lowercase()) {
        "s" -> "varchar(10000)" // TODO exclude constant
        "b" -> "bit"
        "n" -> "bigint" // TODO it should be able to store any number
        "bytebuffer" -> TODO()
        "ss" -> TODO()
        "ns" -> TODO()
        "bs" -> TODO()
        else -> type
    }
}