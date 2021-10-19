package ru.hse.dynamomock.db

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.model.*

@Suppress("unused")
class HSQLDBStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
) : DataStorageLayer {
    private val database = Database.connect("jdbc:h2:mem:$dbname", "org.h2.driver", username, password)

    override fun createTable(tableMetadata: TableMetadata) = transaction(database) {
        SchemaUtils.create(DynamoTable(tableMetadata))
    }

    override fun putItem(request: HSQLDBPutItemRequest) = TODO()

    override fun getItem(request: HSQLDBGetItemRequest): HSQLDBGetItemResponse? = TODO()
}

private class DynamoTable(val metadata: TableMetadata) : Table(metadata.tableName) {
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
