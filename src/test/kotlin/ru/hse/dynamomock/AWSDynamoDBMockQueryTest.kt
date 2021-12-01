package ru.hse.dynamomock

import org.junit.jupiter.api.Test
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.query.grammar.atN
import ru.hse.dynamomock.model.query.grammar.atS
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.*
import java.time.Instant
import kotlin.properties.Delegates
import kotlin.test.assertEquals

internal class AWSDynamoDBMockQueryTest : AWSDynamoDBMockTest() {
    enum class AttributeType { N, S } // TODO test B

    private inner class InsideTest {
        var partKeyType by Delegates.notNull<AttributeType>()
        var sortKeyType: AttributeType? = null

        var tableName: String = "my_table_name"
        var partKey: String = "part_key"
        var sortKey: String = "sort_key"

        var query by Delegates.notNull<QueryRequest>()
        var items by Delegates.notNull<List<Map<String, AttributeValue>>>()

        var expected by Delegates.notNull<Expected>()

        fun test() {
            init()

            val metadata = TableMetadata(
                tableName = tableName,
                attributeDefinitions = listOfNotNull(
                    AttributeDefinition.builder().attributeType(partKeyType.toString()).attributeName(partKey)
                        .build(),
                    sortKeyType?.let {
                        AttributeDefinition.builder().attributeType(it.toString()).attributeName(sortKey).build()
                    }
                ),
                partitionKey = partKey,
                sortKey = sortKeyType?.let { sortKey },
                tableStatus = TableStatus.ACTIVE,
                creationDateTime = Instant.now()
            )

            mock.createTable(metadata.toCreateTableRequest())
            items.forEach { mock.putItem(putItemRequestBuilder(tableName, it)) }
            mock.query(query).apply {
                assertEquals(expected.items.size, count())
                assertEquals(expected.scannedCount, scannedCount())
                assertEquals(expected.items, items())
            }
        }
    }

    private class Expected(val items: List<Map<String, AttributeValue>>, val scannedCount: Int)

    private fun test(autoRun: Boolean = true, block: InsideTest.() -> Unit) {
        InsideTest().apply {
            block()
            if (autoRun) {
                test()
            }
        }
    }

    private fun query(
        tableName: String,
        keyConditions: Map<String, Condition>? = null,
        keyConditionExpression: String? = null,
        queryFilter: Map<String, Condition>? = null,
        filterExpression: String? = null,
        projectionExpression: String? = null,
        attributesToGet: List<String>? = null,
        select: Select? = null,
        scanIndexForward: Boolean? = null,
        expressionAttributeNames: Map<String, String> = emptyMap(),
        expressionAttributeValues: Map<String, AttributeValue> = emptyMap(),
    ): QueryRequest {
        val builder = QueryRequest.builder()
            .tableName(tableName)
            .expressionAttributeNames(expressionAttributeNames)
            .expressionAttributeValues(expressionAttributeValues)
        keyConditions?.let { builder.keyConditions(it) }
        keyConditionExpression?.let { builder.keyConditionExpression(it) }
        queryFilter?.let { builder.queryFilter(it) }
        filterExpression?.let { builder.filterExpression(it) }
        projectionExpression?.let { builder.projectionExpression(it) }
        attributesToGet?.let { builder.attributesToGet(it) }
        select?.let { builder.select(it) }
        scanIndexForward?.let { builder.scanIndexForward(it) }

        return builder.build()
    }

    private fun condition(attributeValueList: List<AttributeValue>, comparisonOperator: ComparisonOperator) =
        Condition.builder().attributeValueList(attributeValueList).comparisonOperator(comparisonOperator).build()

    @Test
    fun `test simple query with partition key`() = test {
        partKeyType = AttributeType.N
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atN("123"), sortKey to atS("kek__!"), "another" to atS("no")),
            mapOf(partKey to atN("1234"), sortKey to atS("no"), "no" to atN("111"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :myVal",
            expressionAttributeValues = mapOf(":myVal" to atN("123"))
        )
        expected = Expected(items = listOf(items[0]), scannedCount = 1)
    }

    @Test
    fun `test comparison num sort key`() = test(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        val value = "value"
        items = listOf(
            mapOf(partKey to atS(value), sortKey to atN("1234")),
            mapOf(partKey to atS(value), sortKey to atN("1000")),
            mapOf(partKey to atS("another"), sortKey to atN("1234")),
            mapOf(partKey to atS(value), sortKey to atN("2000"))
        )

        val comparisons = listOf(EQ, LE, LT, GE, GT)
        val points = listOf(100, 500, 122, 1233, 1234, 1235, 1300, 12000, 13000).map { it.toBigDecimal() }
        for (comparison in comparisons) {
            for (point in points) {
                val expectedItems = items.filter {
                    it[partKey]?.s() == value && comparison.compare(it[sortKey]?.n()?.toBigDecimal()!!, point)
                }

                expected = Expected(items = expectedItems, scannedCount = expectedItems.size)

                query = query(
                    tableName = tableName,
                    keyConditionExpression = "$partKey = :pk and $sortKey ${comparison.toSign()} :sk",
                    expressionAttributeValues = mapOf(":pk" to atS(value), ":sk" to atN(point.toString()))
                )
                test()

                query = query(
                    tableName = tableName,
                    keyConditions = mapOf(
                        partKey to condition(listOf(atS(value)), EQ),
                        sortKey to condition(listOf(atN(point.toString())), comparison)
                    )
                )
                test()
            }
        }
    }

    @Test
    fun `test simple comparison str sort key`() = test {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("a"), sortKey to atS("aa")),
            mapOf(partKey to atS("a"), sortKey to atS("ac"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "#kek = :pp and $sortKey <= :ss",
            expressionAttributeNames = mapOf("#kek" to partKey),
            expressionAttributeValues = mapOf(":pp" to atS("a"), ":ss" to atS("ab"))
        )
        expected = Expected(items = listOf(items[0]), scannedCount = 1)
    }

    @Test
    fun `test begins with sort key`() = test {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("hello"), sortKey to atS("friend")),
            mapOf(partKey to atS("hello"), sortKey to atS("fried chicken!")),
            mapOf(partKey to atS("hello"), sortKey to atS("hello, friend")),
            mapOf(partKey to atS("no"), sortKey to atS("friend"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "begins_with ($sortKey, :start) and $partKey = :hello",
            expressionAttributeValues = mapOf(":start" to atS("frie"), ":hello" to atS("hello"))
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 2)
    }
}

private fun ComparisonOperator.toSign() = when (this) {
    EQ -> "="
    LE -> "<="
    LT -> "<"
    GE -> ">="
    GT -> ">"
    else -> throw IllegalArgumentException()
}

private fun <T : Comparable<T>> ComparisonOperator.compare(arg1: T, arg2: T) = when (this) {
    EQ -> arg1 == arg2
    LE -> arg1 <= arg2
    LT -> arg1 < arg2
    GE -> arg1 >= arg2
    GT -> arg1 > arg2
    else -> throw IllegalArgumentException()
}
