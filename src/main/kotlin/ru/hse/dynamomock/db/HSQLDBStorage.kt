package ru.hse.dynamomock.db

import ru.hse.dynamomock.db.TypesConverter.DEFAULT_TYPE
import ru.hse.dynamomock.db.TypesConverter.fromDynamoToSqlType
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
    private const val ATTRIBUTES_COLUMN_NAME = "Attributes"
    private const val PARTITION_COLUMN_NAME = "PartitionKey"
    private const val SORT_COLUMN_NAME = "SortKey"

    fun createTableQuery(tableMetadata: TableMetadata): String {
        require(tableMetadata.attributeDefinitions.isNotEmpty())
        val partitionKeyType = fromDynamoToSqlType(
            tableMetadata.getAttribute(tableMetadata.partitionKey).attributeTypeAsString()
        )
        val sortKeyType = tableMetadata.sortKey?.let {
            tableMetadata.getAttribute(it).attributeTypeAsString()
        }

        return """
            CREATE TABLE ${tableMetadata.tableName} (
                $ATTRIBUTES_COLUMN_NAME $DEFAULT_TYPE,
                $PARTITION_COLUMN_NAME $partitionKeyType
                ${sortKeyType?.let { ", $SORT_COLUMN_NAME $it" } ?: ""}
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
    const val DEFAULT_TYPE = "varchar(100000)"

    fun fromDynamoToSqlType(type: String): String = when (type.lowercase()) {
        "n" -> "bigint" // TODO more general int type
        else -> DEFAULT_TYPE
    }
}