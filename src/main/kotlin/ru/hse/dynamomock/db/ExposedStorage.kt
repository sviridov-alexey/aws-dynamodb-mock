package ru.hse.dynamomock.db

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.model.*
import java.math.BigDecimal

@Suppress("unused")
class ExposedStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
) : DataStorageLayer {
    private val database = Database.connect("jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1", "org.h2.Driver", username, password)
    private val tables = mutableMapOf<String, DynamoTable>()

    override fun createTable(tableMetadata: TableMetadata) = transaction(database) {
        val table = DynamoTable(tableMetadata)
        tables[tableMetadata.tableName] = table
        SchemaUtils.create(table)
    }

    override fun putItem(request: DBPutItemRequest) {
        transaction(database) {
            val table = tables[request.tableName] ?: throw NullPointerException("table not found")

            table.insert { item ->
                item[attributes] = request.items
                if (request.partitionKey.attributeType == "n") {
                    item[numPartitionKey] = request.partitionKey.attributeValue.toString().toBigDecimal()
                } else {
                    item[stringPartitionKey] = request.partitionKey.attributeValue.toString()
                }

                if (request.sortKey != null) {
                    if (request.sortKey.attributeType == "n") {
                        item[numSortKey] = request.sortKey.attributeValue.toString().toBigDecimal()
                    } else {
                        item[stringSortKey] = request.sortKey.attributeValue.toString()
                    }
                }
            }
        }
    }

    override fun getItem(request: DBGetItemRequest): HSQLDBGetItemResponse? = TODO()
}

class DynamoTable(metadata: TableMetadata) : Table(metadata.tableName) {
    private val id = integer("id").autoIncrement()
    val attributes: Column<String> = text("attributes")
    val stringPartitionKey: Column<String?> = text("stringPartitionKey").nullable().default(null)
    val numPartitionKey: Column<BigDecimal?> = decimal("numPartitionKey", 20, 0).nullable().default(null)
    val stringSortKey: Column<String?> = text("stringSortKey").nullable().default(null)
    val numSortKey: Column<BigDecimal?> = decimal("numSortKey", 20, 0).nullable().default(null)

    override val primaryKey: PrimaryKey = PrimaryKey(id)
}
