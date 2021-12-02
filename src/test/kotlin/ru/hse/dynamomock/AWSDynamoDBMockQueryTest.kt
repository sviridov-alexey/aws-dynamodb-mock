package ru.hse.dynamomock

import org.junit.jupiter.api.Test
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.query.grammar.*
import ru.hse.dynamomock.model.query.grammar.atN
import ru.hse.dynamomock.model.query.grammar.atNS
import ru.hse.dynamomock.model.query.grammar.atS
import ru.hse.dynamomock.model.query.grammar.atSS
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

    @Test
    fun `test between with sort key`() = test {
        partKeyType = AttributeType.N
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atN("2"), sortKey to atS("bcd: wow!!!!")),
            mapOf(partKey to atN("2"), sortKey to atS("be, be, be")),
            mapOf(partKey to atN("2"), sortKey to atS("d, e, f, j")),
            mapOf(partKey to atN("2"), sortKey to atS("abc def"))
        )
        query = query(
            tableName = tableName,
            keyConditions = mapOf(
                partKey to condition(listOf(atN("2")), EQ),
                sortKey to condition(listOf(atS("bark"), atS("carlson")), BETWEEN)
            )
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 2)
    }

    @Test
    fun `test filter expression comparisons`() = test(autoRun = false) {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("wow"), "first" to atN("1234"), "second" to atN("1000")),
            mapOf(partKey to atS("wow"), "first" to atN("4321"), "second" to atN("1")),
            mapOf(partKey to atS("wow"), "first" to atN("1500"), "third" to atN("2"))
        )
        val keyConditionExpression = "$partKey = :val"
        val values = mapOf(":val" to atS("wow"), ":one" to atN("1000"), ":two" to atN("2000"))

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first <= :two and second >= :one",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[0]), scannedCount = 3)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first <= :two or second <= :one",
            expressionAttributeValues = values
        )
        expected = Expected(items = items, scannedCount = 3)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first = :two or third <> :one",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[2]), scannedCount = 3)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first between second and :two and not third = :one",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[0]), scannedCount = 3)
        test()
    }

    @Test
    fun `test filter expression begins with`() = test {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("a"), "kek" to atS("hello")),
            mapOf(partKey to atS("a"), "kek" to atS("hell!!")),
            mapOf(partKey to atS("a"), "kek" to atS("hypothesis")),
            mapOf(partKey to atS("b"), "kek" to atS("hello")),
            mapOf(partKey to atS("a"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :val",
            filterExpression = "begins_with (kek, :start)",
            expressionAttributeValues = mapOf(":val" to atS("a"), ":start" to atS("hell"))
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 4)
    }

    @Test
    fun `test filter expression in`() = test {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("part"), "wow" to atS("1")),
            mapOf(partKey to atS("part"), "wow" to atS("2")),
            mapOf(partKey to atS("part"), "wow" to atN("1")) // It is a number!
        )
        val values = mapOf(":one" to atS("1"), ":another" to atS("3"), ":num" to atN("10"), ":v" to atS("part"))
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "wow in (:one, :v, :another, :num)",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[0]), scannedCount = 3)
    }

    @Test
    fun `test filter expression attributes`() = test(autoRun = false) {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("123"), "a" to atN("1"), "b" to atS("ww")),
            mapOf(partKey to atS("123"), "a" to atS("100"), "b" to atN("13")),
            mapOf(partKey to atS("123"), "b" to atS("-1")),
            mapOf(partKey to atS("124"), "a" to atN("1"), "b" to atS("ww"))
        )
        val values = mapOf(":v" to atS("123"), ":s" to atS("S"))
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "attribute_exists (a)",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 3)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "attribute_not_exists(a)",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[2]), scannedCount = 3)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "attribute_type (b, :s)",
            expressionAttributeValues = values
        )
        expected = Expected(items = listOf(items[0], items[2]), scannedCount = 3)
        test()
    }

    @Test
    fun `test filter expression contains in string and size`() = test(autoRun = false) {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("hello"), "kek" to atS("my dear friend!")),
            mapOf(partKey to atS("hello"), "kek" to atS("friend, how are you?")),
            mapOf(partKey to atS("hello"), "kek" to atS("I love fried chicken!")),
            mapOf(partKey to atS("kek"), "kek" to atS("my dear friend!")),
            mapOf(partKey to atS("hello"), "kek2" to atS("friend!"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "contains (kek, :a)",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":a" to atS("friend"))
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 4)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "size (kek) <= :len",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":len" to atN("15"))
        )
        expected = Expected(items = listOf(items[0]), scannedCount = 4)
        test()
    }

    @Test
    fun `test filter expression contains in string set and size`() = test(autoRun = false) {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("hello"), "kek" to atSS("ab", "ac", "ad"), "val" to atS("ab")),
            mapOf(partKey to atS("hello"), "kek" to atSS("ab", "ac", "ae", "a"), "val" to atS("ae")),
            mapOf(partKey to atS("hello"), "kek" to atSS("ac", "ad"), "val" to atS("ab")),
            mapOf(partKey to atS("kek"), "kek" to atSS("ba", "bc"), "val" to atS("bc")),
            mapOf(partKey to atS("hello"), "kek2" to atSS("ab", "ac", "ad"), "val" to atS("ab"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "contains (kek, val)",
            expressionAttributeValues = mapOf(":v" to atS("hello"))
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 4)
        test()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "size (kek) <> :len",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":len" to atN("4"))
        )
        expected = Expected(items = listOf(items[0], items[2]), scannedCount = 4)
        test()
    }

    @Test
    fun `test filter expression contains in num set`() = test {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("hello"), "kek" to atNS("1", "2", "3")),
            mapOf(partKey to atS("hello"), "kek" to atNS("2", "3", "10")),
            mapOf(partKey to atS("hello"), "kek" to atNS("10", "-1", "3")),
            mapOf(partKey to atS("kek"), "kek" to atNS("10", "100", "2")),
            mapOf(partKey to atS("hello"), "kek2" to atNS("1", "2", "3")),
            mapOf(partKey to atS("hello"), "kek" to atSS("1", "2", "3")) // it is a string set!
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "contains (kek, :value)",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":value" to atN("2"))
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 5)
    }

    @Test
    fun `test filter expression size of lists and maps`() = test {
        partKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("hello"), "kek" to atM("a" to atS("a")), "len" to atN("1")),
            mapOf(partKey to atS("hello"), "kek" to atL(atS("a"), atS("b")), "len" to atN("2")),
            mapOf(partKey to atS("hello"), "kek" to atM("a" to atN("1"), "b" to atS("kek"))),
            mapOf(partKey to atS("kek"), "kek" to atM("a" to atS("a")), "len" to atN("1")),
            mapOf(partKey to atS("hello"), "kek2" to atL(atS("1"), atS("2")), "len" to atN("2")),
            mapOf(partKey to atS("hello"), "kek" to atL(atS("2")), "len" to atN("2"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "size (kek) = len",
            expressionAttributeValues = mapOf(":v" to atS("hello"))
        )
        expected = Expected(items = listOf(items[0], items[1]), scannedCount = 5)
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
