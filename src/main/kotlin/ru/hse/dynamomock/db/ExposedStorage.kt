package ru.hse.dynamomock.db

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.Key

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
        require(tableMetadata.tableName !in tables) {
            "Table ${tableMetadata.tableName} already exists. Cannot create."
        }
        val table = DynamoTable(tableMetadata)
        tables[tableMetadata.tableName] = table
        SchemaUtils.create(table)
    }

    override fun deleteTable(tableName: String) = transaction {
        require(tableName in tables) {
            "Cannot delete non-existent table."
        }
        SchemaUtils.drop(tables.remove(tableName)!!)
    }

    private fun createKeyCondition(
        table: DynamoTable,
        partitionKey: Key,
        sortKey: Key?
    ): (SqlExpressionBuilder.() -> Op<Boolean>) = {

        val partitionOp = when (partitionKey) {
            is StringKey -> table.stringPartitionKey eq partitionKey.attributeValue
            is NumKey -> table.numPartitionKey eq partitionKey.attributeValue
        }

        val sortOp = sortKey?.let {
            when (it) {
                is StringKey -> table.stringSortKey eq it.attributeValue
                is NumKey -> table.numSortKey eq it.attributeValue
            }
        }

        if (sortOp == null) partitionOp else partitionOp and sortOp
    }

    override fun putItem(request: DBPutItemRequest) {
        transaction(database) {
            val table = checkNotNull(tables[request.tableName])
            table.insert { item ->
                item[attributes] = Json.encodeToString(request.items)
                when (request.partitionKey) {
                    is StringKey -> item[stringPartitionKey] = request.partitionKey.attributeValue
                    is NumKey -> item[numPartitionKey] = request.partitionKey.attributeValue
                }

                request.sortKey?.let {
                    when (request.sortKey) {
                        is StringKey -> item[stringSortKey] = request.sortKey.attributeValue
                        is NumKey -> item[numSortKey] = request.sortKey.attributeValue
                    }
                }
            }
        }
    }

    override fun updateItem(request: DBUpdateItemRequest) {
        transaction(database) {
            val table = checkNotNull(tables[request.tableName])
            val condition = createKeyCondition(table, request.partitionKey, request.sortKey)
            table.update(condition) {
                it[attributes] = Json.encodeToString(request.items)
            }
        }
    }

    override fun getItem(request: DBGetItemRequest): List<AttributeInfo> {
        val item = mutableListOf<AttributeInfo>()
        transaction(database) {
            val table = checkNotNull(tables[request.tableName])
            val condition = createKeyCondition(table, request.partitionKey, request.sortKey)
            val info = table.select{condition()}

            info.first().let { item.addAll(Json.decodeFromString(it[table.attributes]))}
        }
        return item
    }

    override fun deleteItem(request: DBDeleteItemRequest) {
        transaction(database) {
            val table = checkNotNull(tables[request.tableName])
            val condition = createKeyCondition(table, request.partitionKey, request.sortKey)
            table.deleteWhere {condition()}
        }
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
