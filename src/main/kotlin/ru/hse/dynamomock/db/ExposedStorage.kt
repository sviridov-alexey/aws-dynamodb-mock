package ru.hse.dynamomock.db

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.model.*

@Suppress("unused")
class ExposedStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
) : DataStorageLayer {
    private val database =
        Database.connect("jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1", "org.h2.Driver", username, password)
    private val tables = mutableMapOf<String, DynamoTable>()

    override fun createTable(tableMetadata: TableMetadata) = transaction(database) {
        val table = DynamoTable(tableMetadata)
        tables[tableMetadata.tableName] = table
        SchemaUtils.create(table)
    }

    override fun putItem(request: DBPutItemRequest) {
        transaction(database) {
            val table = checkNotNull(tables[request.tableName])
            table.insert { item ->
                item[attributes] = Json.encodeToString(request.items)
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

    override fun getItem(request: DBGetItemRequest): List<AttributeInfo> {
        val item = mutableListOf<AttributeInfo>()
        transaction(database) {
            val table = checkNotNull(tables[request.tableName])

            val info = if (request.partitionKey.attributeType == "n") {
                table.select {
                    (table.numPartitionKey eq request.partitionKey.attributeValue.toString().toBigDecimal())
                }
            } else {
                table.select {
                    (table.stringPartitionKey eq request.partitionKey.attributeValue.toString())
                }
            }

            info.first().let { item.addAll(Json.decodeFromString(it[table.attributes]))}
        }
        return item
    }

    class DynamoTable(metadata: TableMetadata) : Table(metadata.tableName) {
        private val id = integer("id").autoIncrement()
        val attributes = text("attributes")
        val stringPartitionKey = text("stringPartitionKey").nullable().default(null)
        val numPartitionKey = decimal("numPartitionKey", 20, 0).nullable().default(null)
        val stringSortKey = text("stringSortKey").nullable().default(null)
        val numSortKey = decimal("numSortKey", 20, 0).nullable().default(null)

        override val primaryKey: PrimaryKey = PrimaryKey(id)
    }
}
