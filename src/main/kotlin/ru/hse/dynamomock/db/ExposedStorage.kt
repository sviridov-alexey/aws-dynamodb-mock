package ru.hse.dynamomock.db

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.*
import ru.hse.dynamomock.model.Key
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator
import software.amazon.awssdk.services.dynamodb.model.Condition
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import java.math.BigDecimal
import java.security.MessageDigest
import java.util.*

class ExposedStorage : DataStorageLayer {
    private val database =
        Database.connect("jdbc:h2:mem:${UUID.randomUUID()};DB_CLOSE_DELAY=-1", "org.h2.Driver", "sa", "")

    private val tables = mutableMapOf<String, DynamoTable>()

    override fun close() = TransactionManager.closeAndUnregister(database)

    override fun createTable(tableMetadata: TableMetadata) {
        val name = hashTableName(tableMetadata.tableName)
        dynamoRequires(name !in tables) {
            "Table $name already exists. Cannot create."
        }
        val table = DynamoTable(tableMetadata)
        transaction(database) { SchemaUtils.create(table) }
        tables[name] = table
    }

    override fun deleteTable(tableName: String) {
        val table = getTable(tableName)
        transaction(database) { SchemaUtils.drop(table) }
        tables.remove(hashTableName(tableName))
    }

    override fun query(tableName: String, keyConditions: Map<String, Condition>?): List<List<AttributeInfo>> {
        val table = getTable(tableName)
        val condition: SqlExpressionBuilder.() -> Op<Boolean> =
            if (keyConditions != null) {
                val keys = keyConditions.map { (name, cond) ->
                    val strs = table.namesToStringColumns.filter { it.first == name }.map { it.second }
                    val nums = table.namesToNumColumns.filter { it.first == name }.map { it.second }

                    strs.firstNotNullOfOrNull { cond.toOp(it) }
                        ?: nums.firstNotNullOfOrNull { cond.toOp(it) }
                        ?: throw dynamoException("Invalid key condition with ${name}.")
                }

                ({ keys.map { it() }.reduce { a, b -> a and b } })
            } else {
                { Op.TRUE }
            }

        return transaction(database) {
            table.select(condition)
                .orderBy(table.stringSortKey to SortOrder.ASC, table.numSortKey to SortOrder.ASC)
                .map { Json.decodeFromString(it[table.attributes]) }
        }
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

                request.fieldValues.forEach { (name, info) ->
                    lsiNames.withIndex().firstOrNull { it.value == name }?.let { (index, _) ->
                        when (val value = info.value) {
                            is BigDecimal -> item[numLsiKeys[index]] = value
                            is String -> item[stringLsiKeys[index]] = value
                            // TODO B
                        }
                    }
                }
            }
        }
    }

    override fun updateItem(request: DBPutItemRequest) {
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
        val numPartitionKey = decimal("numPartitionKey", PRECISION, SCALE).nullable().default(null)
        val stringSortKey = text("stringSortKey").nullable().default(null)
        val numSortKey = decimal("numSortKey", PRECISION, SCALE).nullable().default(null)

        val stringLsiKeys = List(metadata.localSecondaryIndexes.size) { i ->
            text("stringLSISortKey$i").nullable().default(null)
        }
        val numLsiKeys = List(metadata.localSecondaryIndexes.size) { i ->
            decimal("numLSISortKey$i", PRECISION, SCALE).nullable().default(null)
        }

        override val primaryKey: PrimaryKey = PrimaryKey(id)

        val partitionKeyName = metadata.partitionKey
        val sortKeyName = metadata.sortKey
        val lsiNames = metadata.localSecondaryIndexes.map { it.value.sortKey }

        val namesToStringColumns =
            listOf(partitionKeyName to stringPartitionKey, sortKeyName to stringSortKey) + lsiNames.zip(stringLsiKeys)
        val namesToNumColumns =
            listOf(partitionKeyName to numPartitionKey, sortKeyName to numSortKey) + lsiNames.zip(numLsiKeys)
    }

    companion object {
        private const val PRECISION = 20
        private const val SCALE = 0
    }
}

private fun hashTableName(name: String): String =
    MessageDigest.getInstance("MD5").digest(name.toByteArray()).contentToString()

private inline fun <reified T : Comparable<T>> Condition.toOp(
    column: Column<T?>
): (SqlExpressionBuilder.() -> Op<Boolean>)? {
    val args = attributeValueList().map { it.toAttributeTypeInfo().value }.filterIsInstance<T>()
    if (args.size != attributeValueList().size) {
        return null
    }

    val assertArgs = { size: Int ->
        dynamoRequires(args.size == size) {
            "Incorrect number of arguments in a key condition."
        }
    }

    if (comparisonOperator() == ComparisonOperator.BETWEEN) {
        assertArgs(2)
        return { column.between(args[0], args[1]) }
    }

    if (comparisonOperator() == ComparisonOperator.BEGINS_WITH) {
        assertArgs(1)
        if (T::class != String::class) {
            return null
        }
        val arg = args.first() as String
        return {
            @Suppress("UNCHECKED_CAST") // it is checked above :)
            (column as Column<String>).like("$arg%")
        }
    }

    assertArgs(1)
    val arg = args.first()
    return {
        when (comparisonOperator()) {
            ComparisonOperator.EQ -> column eq arg
            ComparisonOperator.LE -> column lessEq arg
            ComparisonOperator.LT -> column less arg
            ComparisonOperator.GE -> column greaterEq arg
            ComparisonOperator.GT -> column greater arg
            else -> throw IllegalStateException()
        }
    }
}
