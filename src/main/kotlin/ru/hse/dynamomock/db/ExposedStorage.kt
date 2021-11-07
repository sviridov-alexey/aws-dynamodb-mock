package ru.hse.dynamomock.db

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.Key
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import java.security.MessageDigest
import java.util.*

class ExposedStorage : DataStorageLayer {
    private val database =
        Database.connect("jdbc:h2:mem:${UUID.randomUUID()};DB_CLOSE_DELAY=-1", "org.h2.Driver", "sa", "")

    private val tables = mutableMapOf<String, DynamoTable>()

    override fun createTable(tableMetadata: TableMetadata) {
        val name = hashTableName(tableMetadata.tableName)
        require(name !in tables) {
            "Table $name already exists. Cannot create."
        }
        val table = DynamoTable(tableMetadata)
        transaction(database) { SchemaUtils.create(table) }
        tables[name] = table
    }

    override fun deleteTable(tableName: String) {
        val name = hashTableName(tableName)
        require(name in tables) {
            "Cannot delete non-existent table."
        }
        transaction(database) { SchemaUtils.drop(tables.getValue(name)) }
        tables.remove(name)
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
        val table = getTable(request.tableName)
        transaction(database) {
            table.insert { item ->
                item[attributes] = Json.encodeToString(request.fieldValues)
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
        val table = getTable(request.tableName)
        transaction(database) {
            val condition = createKeyCondition(table, request.partitionKey, request.sortKey)
            table.update(condition) {
                it[attributes] = Json.encodeToString(request.fieldValues)
            }
        }
    }

    override fun getItem(request: DBGetItemRequest): List<AttributeInfo>? {
        val table = getTable(request.tableName)
        val item = mutableListOf<AttributeInfo>()
        transaction(database) {
            val condition = createKeyCondition(table, request.partitionKey, request.sortKey)
            val info = table.select { condition() }

            val foundItem = info.firstOrNull()
            if (foundItem != null)
                item.addAll(Json.decodeFromString(foundItem[table.attributes]))
        }

        return if (item.isEmpty()) null else item
    }

    override fun deleteItem(request: DBDeleteItemRequest) {
        val table = tables.getValue(hashTableName(request.tableName))
        transaction(database) {
            val condition = createKeyCondition(table, request.partitionKey, request.sortKey)
            table.deleteWhere { condition() }
        }
    }

    private fun getTable(tableName: String): DynamoTable {
        return tables[hashTableName(tableName)] ?: throw ResourceNotFoundException.builder()
            .message(" Cannot do operations on a non-existent table").build()
    }

    private class DynamoTable(metadata: TableMetadata) : Table(hashTableName(metadata.tableName)) {
        private val id = integer("id").autoIncrement()
        val attributes = text("attributes")
        val stringPartitionKey = text("stringPartitionKey").nullable().default(null)
        val numPartitionKey = decimal("numPartitionKey", 20, 0).nullable().default(null)
        val stringSortKey = text("stringSortKey").nullable().default(null)
        val numSortKey = decimal("numSortKey", 20, 0).nullable().default(null)

        override val primaryKey: PrimaryKey = PrimaryKey(id)
    }

    companion object {
        private fun hashTableName(name: String): String =
            MessageDigest.getInstance("MD5").digest(name.toByteArray()).contentToString()
    }
}
