package ru.hse.dynamomock.db

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.model.*

@Suppress("unused")
class H2DBStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
) : DataStorageLayer {
    private val database = Database.connect("jdbc:h2:mem:$dbname", "org.h2.driver", username, password)
    private val tables = mutableMapOf<String, DynamoTable>()

    override fun createTable(tableMetadata: TableMetadata) = transaction(database) {
        val table = DynamoTable(tableMetadata)
        tables[tableMetadata.tableName] = table
        SchemaUtils.create(table)
    }

    override fun putItem(request: DBPutItemRequest) = transaction(database) {
        val table = tables[request.tableName] ?: throw NullPointerException("table not found")

        table.insert{ item ->
            item[attributes] = request.items
            item[partitionKey] = request.partitionKey
            request.sortKey?.let { item[sortKey] = request.sortKey}
        }
    }

    override fun getItem(request: DBGetItemRequest): HSQLDBGetItemResponse? = TODO()
}

class DynamoTable(private val metadata: TableMetadata) : Table(metadata.tableName) {
    val attributes: Column<String> = text("attributes")
    val partitionKey: Column<*> = registerColumn("partitionKey", metadata.partitionKey)
    val sortKey: Column<*> = metadata.sortKey?.let {
        registerColumn("sortKey", it)
    } ?: text("sortKey").nullable().default(null)

    override val primaryKey: PrimaryKey = PrimaryKey(partitionKey)

    private fun registerColumn(columnName: String, attributeName: String): Column<*> =
        when (metadata.getAttribute(attributeName).attributeTypeAsString()) {
            "n" -> decimal(columnName, 20, 0)
            else -> text(columnName)
        }
}
