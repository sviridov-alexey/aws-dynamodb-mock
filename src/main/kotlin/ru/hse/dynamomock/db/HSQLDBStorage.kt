package ru.hse.dynamomock.db

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import ru.hse.dynamomock.db.TypesConverter.DEFAULT_TYPE
import ru.hse.dynamomock.db.TypesConverter.fromDynamoToSqlType
import ru.hse.dynamomock.db.TypesConverter.fromDynamoToValue
import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.HSQLDBGetItemRequest
import ru.hse.dynamomock.model.HSQLDBGetItemResponse
import ru.hse.dynamomock.model.HSQLDBPutItemRequest
import ru.hse.dynamomock.model.Key
import ru.hse.dynamomock.model.TableMetadata
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
        val putItemQuery = SqlQuerier.putItemQuery(request)
        runConnection { prepareStatement(putItemQuery).use { it.execute() } }
    }

    override fun getItem(request: HSQLDBGetItemRequest): HSQLDBGetItemResponse? {

        val getItemQuery = SqlQuerier.getItemQuery(request)

        runConnection {
            prepareStatement(getItemQuery).use {
                val rs = it.executeQuery()
                if (rs.next()) {
                    val jsonValue = rs.getString(SqlQuerier.ATTRIBUTES_COLUMN_NAME)
                    val obj = Json.decodeFromString<List<AttributeInfo>>(jsonValue)
                    return HSQLDBGetItemResponse(obj)
                }
            }
        }
        return null
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
    const val ATTRIBUTES_COLUMN_NAME = "Attributes"
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

    fun putItemQuery(request: HSQLDBPutItemRequest): String {
        return """
            INSERT INTO ${request.tableName}
            VALUES '${request.items}',
            ${fromDynamoToValue(request.partitionKey)}
        ${request.sortKey?.let { ", ${
            fromDynamoToValue(request.sortKey)}"} ?: ""};
        """
    }

    fun getItemQuery(request: HSQLDBGetItemRequest): String {
        return """
             SELECT $ATTRIBUTES_COLUMN_NAME FROM ${request.tableName}
             WHERE $PARTITION_COLUMN_NAME=${fromDynamoToValue(request.partitionKey)}
        """.trimIndent()
    }
}

object TypesConverter {
    const val DEFAULT_TYPE = "varchar(100000)"

    fun fromDynamoToSqlType(type: String): String = when (type.lowercase()) {
        "n" -> "bigint" // TODO more general int type
        else -> DEFAULT_TYPE
    }

    fun fromDynamoToValue(key: Key): Any = when (key.attributeType.lowercase()) {
        "n" -> "${key.attributeValue.toString().toBigDecimal()}"
        else -> "'${key.attributeValue.toString()}'"
    }

}