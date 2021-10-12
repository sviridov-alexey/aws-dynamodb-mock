package ru.hse.dynamomock.db

import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import java.sql.Connection
import java.sql.DriverManager

@Suppress("unused")
class HSQLDBStorage(
    private val dbname: String,
    private val username: String = "sa",
    private val password: String = ""
): DataStorageLayer {
    private inline fun <T> runConnection(action: Connection.() -> T): T =
        connect(dbname, username, password).use(action)

    private inline fun <T> letConnection(action: (Connection) -> T): T =
        runConnection(action)

    override fun createTable(tableMetadata: TableMetadata) {
        val createTableQuery = SqlQuerier.createTableQuery(tableMetadata)
        runConnection { prepareStatement(createTableQuery).use { it.execute() } }
    }

    override fun putItem(request: HSQLDBPutItemRequest) {
        val putItemQuery = SqlQuerier.putItemQuery(request.tableName, request.itemsList)
        runConnection { prepareStatement(putItemQuery).use { it.execute() } }
    }

    override fun getItem(request: HSQLDBGetItemRequest): GetItemResponse {

        val getItemQuery = SqlQuerier.getItemQuery(
            request.tableName,
            request.partitionKey,
            request.attributesToGet
        )
        runConnection {
            prepareStatement(getItemQuery).use {
                val rs = it.executeQuery()
                while (rs.next()) {
                    // TODO: crying in types
                }
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
            values.add(it.attributeValue)
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
            WHERE ${partitionKey.attributeName}=${when (partitionKey.attributeValue) {
            is String -> "'${partitionKey.attributeValue}'"
            is Boolean -> "${partitionKey.attributeValue}"
            // TODO: other types
            else -> "${partitionKey.attributeValue}"
        }};
        """
    }
}

object TypesConverter {
    val defaultType = "varchar(100000)"

    fun fromDynamoToSqlType(type: String): String = when (type.lowercase()) {
        "n" -> "bigint" // TODO more general int type
        else -> defaultType
    }
}