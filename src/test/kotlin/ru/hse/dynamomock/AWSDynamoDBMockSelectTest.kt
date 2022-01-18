package ru.hse.dynamomock

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.query.SelectResponse
import ru.hse.dynamomock.model.query.grammar.*
import ru.hse.dynamomock.model.query.toSelectResponse
import ru.hse.dynamomock.model.sortKey
import ru.hse.dynamomock.model.toAttributeTypeInfo
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.*
import java.math.BigDecimal
import java.time.Instant
import java.util.*
import kotlin.NoSuchElementException
import kotlin.properties.Delegates
import kotlin.test.*

internal class AWSDynamoDBMockSelectTest : AWSDynamoDBMockTest() {
    enum class AttributeType { N, S } // TODO test B

    private inner class InsideTest {
        var partKeyType by Delegates.notNull<AttributeType>()
        var sortKeyType: AttributeType? = null

        var tableName: String = "my_table_name"
        var partKey: String = "part_key"
        var sortKey: String = "sort_key"
        var defaultLsiName: String = "default_index_name"

        var query by Delegates.notNull<QueryRequest>()
        var scan by Delegates.notNull<ScanRequest>()
        var items by Delegates.notNull<List<Map<String, AttributeValue>>>()
        var localSecondaryIndexes = emptyList<Pair<LocalSecondaryIndex, AttributeType>>()

        var expected by Delegates.notNull<Expected>()

        private fun buildMetadata() = TableMetadata(
            tableName = tableName,
            attributeDefinitions = listOfNotNull(
                AttributeDefinition.builder().attributeType(partKeyType.toString()).attributeName(partKey).build(),
                sortKeyType?.let {
                    AttributeDefinition.builder().attributeType(it.toString()).attributeName(sortKey).build()
                }
            ) + localSecondaryIndexes.map { (index, type) ->
                AttributeDefinition.builder().attributeType(type.toString()).attributeName(index.sortKey).build()
            },
            partitionKey = partKey,
            sortKey = sortKeyType?.let { sortKey },
            tableStatus = TableStatus.ACTIVE,
            localSecondaryIndexes = localSecondaryIndexes.associate { it.first.indexName() to it.first },
            creationDateTime = Instant.now()
        )

        private fun prepare() {
            val metadata = buildMetadata()
            mock.createTable(metadata.toCreateTableRequest())
            items.forEach { mock.putItem(putItemRequestBuilder(tableName, it)) }
        }

        fun testQuery() {
            init()
            prepare()
            mock.query(query).apply {
                assertNotNull(items())
                assertNotNull(lastEvaluatedKey())
                expected.assert(toSelectResponse())
            }
        }

        fun testScan() {
            init()
            prepare()
            mock.scan(scan).apply {
                assertNotNull(items())
                assertNotNull(lastEvaluatedKey())
                expected.assert(toSelectResponse())
            }
        }

        fun localSecondaryIndex(
            attributeName: String,
            projectionType: ProjectionType,
            nonKeyAttributes: List<String>,
            indexName: String = defaultLsiName,
        ): LocalSecondaryIndex = LocalSecondaryIndex
            .builder()
            .indexName(indexName)
            .keySchema(
                KeySchemaElement.builder().attributeName(partKey).keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName(attributeName).keyType(KeyType.RANGE).build()
            )
            .projection(Projection.builder().projectionType(projectionType).nonKeyAttributes(nonKeyAttributes).build())
            .build()
    }

    private sealed interface Expected {
        fun assert(response: SelectResponse)
    }

    private data class ExpectedItems(
        val items: List<Map<String, AttributeValue>>,
        val scannedCount: Int,
        val ignoreOrder: Boolean = false
    ) : Expected {
        override fun assert(response: SelectResponse) {
            assertEquals(items.size, response.count)
            assertEquals(scannedCount, response.scannedCount)
            if (!ignoreOrder) {
                assertEquals(items, response.items)
            } else {
                assertEquals(items.toSet(), response.items.toSet())
            }
            assertNotNull(response.lastEvaluatedKey)
        }
    }

    private data class ExpectedCount(val count: Int, val scannedCount: Int) : Expected {
        override fun assert(response: SelectResponse) {
            assertNull(response.items)
            assertNull(response.lastEvaluatedKey)
            assertEquals(count, response.count)
            assertEquals(scannedCount, response.scannedCount)
        }
    }

    private object EmptyExpected : Expected {
        override fun assert(response: SelectResponse) = Unit
    }

    private inline fun testQuery(autoRun: Boolean = true, block: InsideTest.() -> Unit) {
        InsideTest().apply {
            block()
            if (autoRun) testQuery()
        }
    }

    private inline fun testScan(autoRun: Boolean = true, block: InsideTest.() -> Unit) {
        InsideTest().apply {
            block()
            if (autoRun) testScan()
        }
    }

    private inline fun <reified T : Exception> queryFailed(block: InsideTest.() -> Unit) {
        assertThrows<T> {
            testQuery {
                partKeyType = AttributeType.S
                items = listOf()
                expected = EmptyExpected
                block()
            }
        }
    }

    private fun query(
        tableName: String,
        indexName: String? = null,
        keyConditions: Map<String, Condition>? = null,
        keyConditionExpression: String? = null,
        queryFilter: Map<String, Condition>? = null,
        conditionalOperator: ConditionalOperator? = null,
        filterExpression: String? = null,
        projectionExpression: String? = null,
        attributesToGet: List<String>? = null,
        select: Select? = null,
        scanIndexForward: Boolean? = null,
        limit: Int? = null,
        exclusiveStartKey: Map<String, AttributeValue>? = null,
        expressionAttributeNames: Map<String, String> = emptyMap(),
        expressionAttributeValues: Map<String, AttributeValue> = emptyMap(),
    ): QueryRequest {
        val builder = QueryRequest.builder()
            .tableName(tableName)
            .expressionAttributeNames(expressionAttributeNames)
            .expressionAttributeValues(expressionAttributeValues)
        indexName?.let { builder.indexName(it) }
        keyConditions?.let { builder.keyConditions(it) }
        keyConditionExpression?.let { builder.keyConditionExpression(it) }
        queryFilter?.let { builder.queryFilter(it) }
        conditionalOperator?.let { builder.conditionalOperator(it) }
        filterExpression?.let { builder.filterExpression(it) }
        projectionExpression?.let { builder.projectionExpression(it) }
        attributesToGet?.let { builder.attributesToGet(it) }
        select?.let { builder.select(it) }
        scanIndexForward?.let { builder.scanIndexForward(it) }
        limit?.let { builder.limit(it) }
        exclusiveStartKey?.let { builder.exclusiveStartKey(it) }

        return builder.build()
    }

    private fun condition(attributeValueList: List<AttributeValue>, comparisonOperator: ComparisonOperator) =
        Condition.builder().attributeValueList(attributeValueList).comparisonOperator(comparisonOperator).build()

    @Test
    fun `test simple query with partition key`() = testQuery {
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
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 1)
    }

    @Test
    fun `test comparison num sort key`() = testQuery(autoRun = false) {
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
                    it.getValue(partKey).s() == value && comparison.compare(it.getValue(sortKey).n().toBigDecimal(), point)
                }.sortedBy { it.getValue(sortKey).toAttributeTypeInfo().value as BigDecimal }

                expected = ExpectedItems(items = expectedItems, scannedCount = expectedItems.size)

                query = query(
                    tableName = tableName,
                    keyConditionExpression = "$partKey = :pk and $sortKey ${comparison.toSign()} :sk",
                    expressionAttributeValues = mapOf(":pk" to atS(value), ":sk" to atN(point.toString()))
                )
                testQuery()

                query = query(
                    tableName = tableName,
                    keyConditions = mapOf(
                        partKey to condition(listOf(atS(value)), EQ),
                        sortKey to condition(listOf(atN(point.toString())), comparison)
                    )
                )
                testQuery()
            }
        }
    }

    @Test
    fun `test simple comparison str sort key`() = testQuery {
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
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 1)
    }

    @Test
    fun `test begins with sort key`() = testQuery {
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
        expected = ExpectedItems(items = listOf(items[1], items[0]), scannedCount = 2)
    }

    @Test
    fun `test between with sort key`() = testQuery {
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
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 2)
    }

    @Test
    fun `test filter expression comparisons`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(partKey to atS("wow"), sortKey to atN("1"), "first" to atN("1234"), "second" to atN("1000")),
            mapOf(partKey to atS("wow"), sortKey to atN("2"), "first" to atN("4321"), "second" to atN("1")),
            mapOf(partKey to atS("wow"), sortKey to atN("3"), "first" to atN("1500"), "third" to atN("2"))
        )
        val keyConditionExpression = "$partKey = :val"
        val values = mapOf(":val" to atS("wow"), ":one" to atN("1000"), ":two" to atN("2000"))

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first <= :two and second >= :one",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 3)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first <= :two or second <= :one",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = items, scannedCount = 3)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first = :two or third <> :one",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[2]), scannedCount = 3)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            filterExpression = "first between second and :two and not third = :one",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 3)
        testQuery()
    }

    @Test
    fun `test filter expression begins with`() = testQuery {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("a"), sortKey to atS("a"), "kek" to atS("hello")),
            mapOf(partKey to atS("a"), sortKey to atS("b"), "kek" to atS("hell!!")),
            mapOf(partKey to atS("a"), sortKey to atS("c"), "kek" to atS("hypothesis")),
            mapOf(partKey to atS("b"), sortKey to atS("d"), "kek" to atS("hello")),
            mapOf(partKey to atS("a"), sortKey to atS("e"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :val",
            filterExpression = "begins_with (kek, :start)",
            expressionAttributeValues = mapOf(":val" to atS("a"), ":start" to atS("hell"))
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 4)
    }

    @Test
    fun `test filter expression in`() = testQuery {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("part"), sortKey to atS("1"), "wow" to atS("1")),
            mapOf(partKey to atS("part"), sortKey to atS("2"), "wow" to atS("2")),
            mapOf(partKey to atS("part"), sortKey to atS("3"), "wow" to atN("1")) // It is a number!
        )
        val values = mapOf(":one" to atS("1"), ":another" to atS("3"), ":num" to atN("10"), ":v" to atS("part"))
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "wow in (:one, :v, :another, :num)",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 3)
    }

    @Test
    fun `test filter expression attributes`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("123"), sortKey to atS("1"), "a" to atN("1"), "b" to atS("ww")),
            mapOf(partKey to atS("123"), sortKey to atS("2"), "a" to atS("100"), "b" to atN("13")),
            mapOf(partKey to atS("123"), sortKey to atS("3"), "b" to atS("-1")),
            mapOf(partKey to atS("124"), sortKey to atS("4"), "a" to atN("1"), "b" to atS("ww"))
        )
        val values = mapOf(":v" to atS("123"), ":s" to atS("S"))
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "attribute_exists (a)",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 3)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "attribute_not_exists(a)",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[2]), scannedCount = 3)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "attribute_type (b, :s)",
            expressionAttributeValues = values
        )
        expected = ExpectedItems(items = listOf(items[0], items[2]), scannedCount = 3)
        testQuery()
    }

    @Test
    fun `test filter expression contains in string and size`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(partKey to atS("hello"), sortKey to atN("1"), "kek" to atS("my dear friend!")),
            mapOf(partKey to atS("hello"), sortKey to atN("10"), "kek" to atS("friend, how are you?")),
            mapOf(partKey to atS("hello"), sortKey to atN("100"), "kek" to atS("I love fried chicken!")),
            mapOf(partKey to atS("kek"), sortKey to atN("20"), "kek" to atS("my dear friend!")),
            mapOf(partKey to atS("hello"), sortKey to atN("3"), "kek2" to atS("friend!"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "contains (kek, :a)",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":a" to atS("friend"))
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 4)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "size (kek) <= :len",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":len" to atN("15"))
        )
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 4)
        testQuery()
    }

    @Test
    fun `test filter expression contains in string set and size`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(partKey to atS("hello"), sortKey to atN("1"), "kek" to atSS("ab", "ac", "ad"), "val" to atS("ab")),
            mapOf(
                partKey to atS("hello"),
                sortKey to atN("2"),
                "kek" to atSS("ab", "ac", "ae", "a"),
                "val" to atS("ae")
            ),
            mapOf(partKey to atS("hello"), sortKey to atN("3"), "kek" to atSS("ac", "ad"), "val" to atS("ab")),
            mapOf(partKey to atS("kek"), sortKey to atN("4"), "kek" to atSS("ba", "bc"), "val" to atS("bc")),
            mapOf(partKey to atS("hello"), sortKey to atN("5"), "kek2" to atSS("ab", "ac", "ad"), "val" to atS("ab"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "contains (kek, val)",
            expressionAttributeValues = mapOf(":v" to atS("hello"))
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 4)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "size (kek) <> :len",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":len" to atN("4"))
        )
        expected = ExpectedItems(items = listOf(items[0], items[2]), scannedCount = 4)
        testQuery()
    }

    @Test
    fun `test filter expression contains in num set`() = testQuery {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(partKey to atS("hello"), sortKey to atN("1"), "kek" to atNS("1", "2", "3")),
            mapOf(partKey to atS("hello"), sortKey to atN("2"), "kek" to atNS("2", "3", "10")),
            mapOf(partKey to atS("hello"), sortKey to atN("3"), "kek" to atNS("10", "-1", "3")),
            mapOf(partKey to atS("kek"), sortKey to atN("6"), "kek" to atNS("10", "100", "2")),
            mapOf(partKey to atS("hello"), sortKey to atN("7"), "kek2" to atNS("1", "2", "3")),
            mapOf(partKey to atS("hello"), sortKey to atN("8"), "kek" to atSS("1", "2", "3")) // it is a string set!
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "contains (kek, :value)",
            expressionAttributeValues = mapOf(":v" to atS("hello"), ":value" to atN("2"))
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 5)
    }

    @Test
    fun `test filter expression size of lists and maps`() = testQuery {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("hello"), sortKey to atS("a"), "kek" to atM("a" to atS("a")), "len" to atN("1")),
            mapOf(partKey to atS("hello"), sortKey to atS("b"), "kek" to atL(atS("a"), atS("b")), "len" to atN("2")),
            mapOf(partKey to atS("hello"), sortKey to atS("c"), "kek" to atM("a" to atN("1"), "b" to atS("kek"))),
            mapOf(partKey to atS("kek"), sortKey to atS("d"), "kek" to atM("a" to atS("a")), "len" to atN("1")),
            mapOf(partKey to atS("hello"), sortKey to atS("x"), "kek2" to atL(atS("1"), atS("2")), "len" to atN("2")),
            mapOf(partKey to atS("hello"), sortKey to atS("y"), "kek" to atL(atS("2")), "len" to atN("2"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "size (kek) = len",
            expressionAttributeValues = mapOf(":v" to atS("hello"))
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 5)
    }

    @Test
    fun `test simple query filter`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.N
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(
                partKey to atN("-10"),
                sortKey to atN("1"),
                "one" to atS("what"),
                "list" to atSS("one", "two"),
                "val" to atN("73")
            ),
            mapOf(
                partKey to atN("-10"),
                sortKey to atN("2"),
                "one1" to atN("10"),
                "list" to atSS("two", "three"),
                "val" to atN("37")
            )
        )
        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atN("-10")), EQ)),
            queryFilter = mapOf("one" to condition(listOf(), NOT_NULL))
        )
        expected = ExpectedItems(items = listOf(items[0]), scannedCount = 2)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atN("-10")), EQ)),
            queryFilter = mapOf("one" to condition(listOf(), NOT_NULL), "one1" to condition(listOf(), NOT_NULL)),
            conditionalOperator = ConditionalOperator.OR
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 2)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atN("-10")), EQ)),
            queryFilter = mapOf("one" to condition(listOf(), NOT_NULL), "one1" to condition(listOf(), NOT_NULL))
        )
        expected = ExpectedItems(items = listOf(), scannedCount = 2)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atN("-10")), EQ)),
            queryFilter = mapOf("list" to condition(listOf(atS("two")), CONTAINS))
        )
        expected = ExpectedItems(items = listOf(items[0], items[1]), scannedCount = 2)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atN("-10")), EQ)),
            queryFilter = mapOf("val" to condition(listOf(atN("50")), LT))
        )
        expected = ExpectedItems(items = listOf(items[1]), scannedCount = 2)
        testQuery()
    }

    @Test
    fun `test filter expression complex`() = testQuery {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(partKey to atS("seven"), sortKey to atN("1"), "one" to atN("10"), "two" to atS("seven")),
            mapOf(partKey to atS("seven"), sortKey to atN("2"), "list" to atL(atN("10"), atS("-10"), atSS("kek"))),
            mapOf(partKey to atS("seven"), sortKey to atN("3"), "set" to atNS("2", "1")),
            mapOf(partKey to atS("seven"), sortKey to atN("4"), "list" to atL(atS("10"), atN("-10"), atSS("kek"))),
            mapOf(partKey to atS("seven"), sortKey to atN("5"), "set" to atNS("1", "2", "3")),
            mapOf(partKey to atS("seven"), sortKey to atN("6"), "set" to atSS("1", "2")),
            mapOf(partKey to atS("seven"), sortKey to atN("7"), "one" to atN("5"), "two" to atS("seven")),
            mapOf(partKey to atS("seven"), sortKey to atN("9"), "one" to atN("10"), "two" to atS("Seven")),
            mapOf(partKey to atS("Seven"), sortKey to atN("10"), "one" to atN("10"), "two" to atS("seven"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = ":v in (one, two, :t, not_) and :a between :b and one or not not :c <> list or :s = set",
            expressionAttributeValues = mapOf(
                ":v" to atS("seven"),
                ":t" to atN("10000000"),
                ":a" to atN("7"),
                ":b" to atN("-7"),
                ":s" to atNS("1", "2"),
                ":c" to atL(atS("10"), atN("-10"), atSS("kek"))
            )
        )
        expected = ExpectedItems(items = listOf(items[0], items[1], items[2]), scannedCount = 8)
    }

    @Test
    fun `test filter expression nested attributes`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.N
        items = listOf(mapOf(
            partKey to atN("1"),
            "map" to atM(
                "one" to atL(atL(atS("10"), atN("-2"))),
                "hello" to atL(atN("-2"), atL(atS("hi"), atS("no"))),
                "other" to atL(atN("-2"), atM("nested" to atL(atS("no"))))
            )
        ))
        val keyConditionExpression = "$partKey = :v"
        val values = mapOf(
            ":v" to atN("1"), ":hi" to atS("hi"),
            ":no" to atS("no"), ":ten" to atS("10"),
            ":two" to atN("-2"), ":list" to atL(atS("hi"), atS("no"))
        )

        fun test(filterExpression: String, expectedItems: List<Map<String, AttributeValue>>) {
            query = query(
                tableName = tableName,
                keyConditionExpression = keyConditionExpression,
                filterExpression = filterExpression,
                expressionAttributeValues = values
            )
            expected = ExpectedItems(items = expectedItems, scannedCount = 1)
            testQuery()
        }
        fun testSuccess(filterExpression: String) = test(filterExpression, items)
        fun testFail(filterExpression: String) = test(filterExpression, listOf())

        testSuccess("map.one[0][0] = :ten")
        testSuccess("map.one[0][1] = :two")
        testSuccess("map.hello[1][0] = :hi")
        testSuccess("map.other[1].nested[0] = :no")
        testSuccess("map.other[0] = :two")
        testSuccess("map.hello[1] = :list")

        testFail("map.one[10] = :ten")
        testFail("map.other[0].nested = :two")
        testFail("hello = :ten")
        testFail("map.hello[1] = :hi")
    }

    @Test
    fun `test scan index forward`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        items = listOf(
            mapOf(partKey to atS("q"), sortKey to atN("10")),
            mapOf(partKey to atS("q"), sortKey to atN("-15")),
            mapOf(partKey to atS("q"), sortKey to atN("20")),
            mapOf(partKey to atS("b"), sortKey to atN("13"))
        )
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :q",
            expressionAttributeValues = mapOf(":q" to atS("q"))
        )
        expected = ExpectedItems(items = listOf(items[1], items[0], items[2]), scannedCount = 3)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :q",
            expressionAttributeValues = mapOf(":q" to atS("q")),
            scanIndexForward = false
        )
        expected = ExpectedItems(items = listOf(items[2], items[0], items[1]), scannedCount = 3)
        testQuery()
    }

    @Test
    fun `test attributes to get`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.N
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(
                partKey to atN("10"),
                sortKey to atS("1"),
                "a" to atN("7"),
                "b" to atS("13"),
                "c" to atL(atS("20"), atS("-13"))
            ),
            mapOf(
                partKey to atN("10"),
                sortKey to atS("2"),
                "a" to atS("13"),
                "b" to atS("kek"),
                "d" to atSS("one", "two")
            ),
            mapOf(partKey to atN("10"), sortKey to atS("3"), "b" to atNS("10", "1", "2")),
            mapOf(partKey to atN("10"), sortKey to atS("4"), "e" to atN("-1")),
            mapOf(partKey to atN("0"), sortKey to atS("5"), "a" to atN("10")),
        )
        val keyConditions = mapOf(partKey to condition(listOf(atN("10")), EQ))
        query = query(
            tableName = tableName,
            keyConditions = keyConditions,
            attributesToGet = listOf("a", "b", "d")
        )
        expected = ExpectedItems(scannedCount = 4, items = listOf(
            mapOf("a" to atN("7"), "b" to atS("13")),
            mapOf("a" to atS("13"), "b" to atS("kek"), "d" to atSS("one", "two")),
            mapOf("b" to atNS("10", "1", "2")),
            mapOf()
        ))
        testQuery()

        query = query(
            tableName = tableName,
            keyConditions = keyConditions,
            attributesToGet = listOf("d"),
            select = Select.SPECIFIC_ATTRIBUTES
        )
        expected = ExpectedItems(scannedCount = 4, items = listOf(
            mapOf(),
            mapOf("d" to atSS("one", "two")),
            mapOf(),
            mapOf()
        ))
        testQuery()
    }

    @Test
    fun `test projection expression`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.N
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atN("10"), sortKey to atS("a"), "a" to atN("7"), "b" to atS("13")),
            mapOf(partKey to atN("10"), sortKey to atS("w"), "a" to atS("13"), "b" to atS("a"), "d" to atSS("1", "2")),
            mapOf(partKey to atN("10"), sortKey to atS("x"), "b" to atNS("10", "1", "2")),
            mapOf(partKey to atN("10"), sortKey to atS("y"), "e" to atN("-1")),
            mapOf(partKey to atN("0"), sortKey to atS("z"), "a" to atN("10")),
        )
        val keyConditionExpression = "$partKey = :v"
        val values = mapOf(":v" to atN("10"))
        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            expressionAttributeValues = values,
            projectionExpression = "a,\t\t b"
        )
        expected = ExpectedItems(scannedCount = 4, items = listOf(
            mapOf("a" to atN("7"), "b" to atS("13")),
            mapOf("a" to atS("13"), "b" to atS("a")),
            mapOf("b" to atNS("10", "1", "2")),
            mapOf()
        ))
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            expressionAttributeValues = values,
            projectionExpression = "b",
            select = Select.SPECIFIC_ATTRIBUTES
        )
        expected = ExpectedItems(scannedCount = 4, items = listOf(
            mapOf("b" to atS("13")),
            mapOf("b" to atS("a")),
            mapOf("b" to atNS("10", "1", "2")),
            mapOf()
        ))
        testQuery()
    }

    @Test
    fun `test projection expression nested attributes`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.N
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(
                partKey to atN("0"),
                sortKey to atS("1"),
                "a" to atM("f" to atL(atS("a"), atN("2"))),
                "b" to atL(atM("f" to atS("k"), "s" to atN("20")))
            ),
            mapOf(
                partKey to atN("0"),
                sortKey to atS("11"),
                "a" to atM("f" to atM("f" to atS("kek"), "s" to atL(atS("kek"), atN("0")))),
                "b" to atL(atM("f" to atN("20")), atL(atL(atS("kek"))))
            )
        )
        val keyConditionExpression = "$partKey = :v"
        val values = mapOf(":v" to atN("0"))
        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            expressionAttributeValues = values,
            projectionExpression = "a.f, a.s,b[0].f, b[1],    $partKey, $sortKey"
        )
        expected = ExpectedItems(items = items, scannedCount = 2)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            expressionAttributeValues = values,
            projectionExpression = "a.f.s[1]"
        )
        expected = ExpectedItems(scannedCount = 2, items = listOf(
            mapOf(),
            mapOf("a" to items[1].getValue("a"))
        ))
        testQuery()
    }

    @Test
    fun `test select`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("a"), sortKey to atS("b"), "a" to atN("20"), "b" to atS("kek")),
            mapOf(partKey to atS("a"), sortKey to atS("a"), "a" to atS("20"), "c" to atN("20"))
        )
        val keyConditionExpression = "$partKey = :v"
        val values = mapOf(":v" to atS("a"))
        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            expressionAttributeValues = values,
            scanIndexForward = false,
            select = Select.ALL_ATTRIBUTES
        )
        expected = ExpectedItems(items = items, scannedCount = 2)
        testQuery()

        query = query(
            tableName = tableName,
            keyConditionExpression = keyConditionExpression,
            expressionAttributeValues = values,
            select = Select.COUNT
        )
        expected = ExpectedCount(count = 2, scannedCount = 2)
    }

    @Test
    fun `test limit, exclusive start key and last evaluated key`() {
        val tableName = "table"
        val partKey = "part"
        val sortKey = "sort"
        val items = (0..100).shuffled(Random(100)).map {
            mutableMapOf(partKey to atS("val"), sortKey to atN(it.toString())).apply {
                if (it % 3 == 0) {
                    this += "kek" to atS("wow")
                }
            }.toMap()
        }
        val expectedItems = items.filter { "kek" !in it }.sortedBy { it.getValue(sortKey).n().toBigDecimal() }
        val metadata = TableMetadata(
            tableName = tableName,
            attributeDefinitions = listOf(
                AttributeDefinition.builder().attributeType("S").attributeName(partKey).build(),
                AttributeDefinition.builder().attributeType("N").attributeName(sortKey).build()
            ),
            partitionKey = partKey,
            sortKey = sortKey,
            tableStatus = TableStatus.ACTIVE,
            localSecondaryIndexes = emptyMap(),
            creationDateTime = Instant.now()
        )

        mock.createTable(metadata.toCreateTableRequest())
        items.forEach { mock.putItem(putItemRequestBuilder(tableName, it)) }

        val limit = 7
        var exclusiveStartKey: Map<String, AttributeValue>? = null
        val result = mutableListOf<Map<String, AttributeValue>>()
        while (result.size < expectedItems.size) {
            val response = mock.query(query(
                tableName = tableName,
                keyConditionExpression = "$partKey = :v",
                filterExpression = "attribute_not_exists(kek)",
                expressionAttributeValues = mapOf(":v" to atS("val")),
                limit = limit,
                exclusiveStartKey = exclusiveStartKey
            ))
            assertTrue(response.hasItems())
            assertTrue(response.count() <= response.scannedCount())
            assertEquals(response.count(), response.items().size)
            assertTrue(response.count() in 1..limit)
            result += response.items()
            exclusiveStartKey = response.lastEvaluatedKey().takeIf { response.hasLastEvaluatedKey() }
        }

        assertEquals(expectedItems, result)
    }

    @Test
    fun `test local secondary indexes with sort key`() = testQuery(autoRun = false) {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.N
        val index = "ind"
        localSecondaryIndexes = listOf(
            localSecondaryIndex(index, ProjectionType.KEYS_ONLY, emptyList()) to AttributeType.S
        )
        items = listOf(
            mapOf(partKey to atS("wow"), sortKey to atN("11"), index to atS("what"), "a" to atN("1")),
            mapOf(partKey to atS("wow"), sortKey to atN("10"), index to atS("no"), "b" to atS("a")),
            mapOf(partKey to atS("wow"), sortKey to atN("-1"), index to atS("what"), "c" to atSS("a", "b")),
            mapOf(partKey to atS("No wow"), sortKey to atN("4"), index to atS("what")),
            mapOf(partKey to atS("no wow"), sortKey to atN("5"))
        )
        query = query(
            tableName = tableName,
            indexName = defaultLsiName,
            keyConditionExpression = "$partKey = :val and $index = :other",
            expressionAttributeValues = mapOf(":val" to atS("wow"), ":other" to atS("what"))
        )
        expected = ExpectedItems(
            items = listOf(items[0], items[2]).map { item ->
                item.filterKeys { it in listOf(partKey, sortKey, index) }
            },
            scannedCount = 2,
            ignoreOrder = true
        )
        testQuery()

        localSecondaryIndexes = listOf(
            localSecondaryIndex(index, ProjectionType.ALL, emptyList()) to AttributeType.S
        )
        expected = ExpectedItems(items = listOf(items[0], items[2]), scannedCount = 2, ignoreOrder = true)
        testQuery()

        localSecondaryIndexes = listOf(
            localSecondaryIndex(index, ProjectionType.INCLUDE, listOf("a")) to AttributeType.S
        )
        expected = ExpectedItems(
            items = listOf(items[0], items[2]).map { item ->
                item.filterKeys { it in listOf(partKey, sortKey, index, "a") }
            },
            scannedCount = 2,
            ignoreOrder = true
        )
        testQuery()

        localSecondaryIndexes = listOf(
            localSecondaryIndex(index, ProjectionType.KEYS_ONLY, emptyList()) to AttributeType.S
        )
        query = query.toBuilder().select(Select.ALL_ATTRIBUTES).build()
        expected = ExpectedItems(items = listOf(items[0], items[2]), scannedCount = 2, ignoreOrder = true)
        testQuery()
    }

    @Test
    fun `test local secondary indexes without sort key`() = queryFailed<DynamoDbException> {
        partKeyType = AttributeType.S
        val index = "myLovelyIndex"
        localSecondaryIndexes = listOf(
            localSecondaryIndex(index, ProjectionType.ALL, emptyList()) to AttributeType.S
        )
        query = query(
            tableName = tableName,
            indexName = defaultLsiName,
            keyConditionExpression = "$partKey = :val and myLovelyIndex = :other",
            expressionAttributeValues = mapOf(":val" to atS("wow"), ":other" to atS("what")),
            select = Select.ALL_PROJECTED_ATTRIBUTES
        )
    }

    @Test
    fun `test fail non existent table`() = queryFailed<DynamoDbException> {
        query = query(
            tableName = "non-existent",
            keyConditionExpression = "$partKey = :v",
            expressionAttributeValues = mapOf(":v" to atS("a"))
        )
    }

    @Test
    fun `test fail select and attributes to get`() {
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditions = mapOf(partKey to condition(listOf(atS("a")), EQ)),
                attributesToGet = listOf("a"),
                select = Select.ALL_ATTRIBUTES
            )
        }
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditions = mapOf(partKey to condition(listOf(atS("a")), EQ)),
                attributesToGet = listOf("a"),
                select = Select.COUNT
            )
        }
    }

    @Test
    fun `test fail select and projection expression`() {
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditionExpression = "$partKey = :v",
                expressionAttributeValues = mapOf(":v" to atS("a")),
                projectionExpression = "a",
                select = Select.ALL_ATTRIBUTES
            )
        }
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditionExpression = "$partKey = :v",
                expressionAttributeValues = mapOf(":v" to atS("a")),
                projectionExpression = "a",
                select = Select.COUNT
            )
        }
    }

    @Test
    fun `test fail same in attributes to get`() {
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditions = mapOf(partKey to condition(listOf(atS("a")), EQ)),
                attributesToGet = listOf("a", "a")
            )
        }
    }

    @Test
    fun `test fail attributes to get and projection expression at the same time`() = queryFailed<DynamoDbException> {
        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atS("a")), EQ)),
            attributesToGet = listOf("a"),
            projectionExpression = "a"
        )
    }

    @Test
    fun `test fail key conditions no partition key`() {
        queryFailed<DynamoDbException> {
            sortKeyType = AttributeType.S
            query = query(
                tableName = tableName,
                keyConditions = mapOf(sortKey to condition(listOf(atS("a")), EQ))
            )
        }
        queryFailed<DynamoDbException> {
            sortKeyType = AttributeType.S
            query = query(
                tableName = tableName,
                keyConditionExpression = "$sortKey = :v",
                expressionAttributeValues = mapOf(":v" to atS("a"))
            )
        }
    }

    @Test
    fun `test fail key conditions extra attribute`() = queryFailed<DynamoDbException> {
        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atS("a")), EQ), "a" to condition(listOf(atS("a")), EQ))
        )
    }

    @Test
    fun `test fail key conditions invalid number of arguments`() = queryFailed<DynamoDbException> {
        query = query(
            tableName = tableName,
            keyConditions = mapOf(partKey to condition(listOf(atS("a"), atS("b")), EQ))
        )
    }

    @Test
    fun `test fail key conditions not eq on partition key`() {
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditions = mapOf(partKey to condition(listOf(atS("a")), LE))
            )
        }
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditionExpression = "$partKey <= :v",
                expressionAttributeValues = mapOf(":v" to atS("a"))
            )
        }
    }

    @Test
    fun `test fail key conditions invalid operation on sort key`() {
        queryFailed<DynamoDbException> {
            sortKeyType = AttributeType.N
            query = query(
                tableName = tableName,
                keyConditions = mapOf(
                    partKey to condition(listOf(atS("a")), EQ),
                    sortKey to condition(listOf(), NOT_NULL)
                )
            )
        }
        queryFailed<DynamoDbException> {
            sortKeyType = AttributeType.N
            query = query(
                tableName = tableName,
                keyConditionExpression = "$partKey = :a and attribute_exists ($sortKey)",
                expressionAttributeValues = mapOf(":a" to atS("a"))
            )
        }
    }

    @Test
    fun `test fail key condition expression not and`() = queryFailed<DynamoDbException> {
        sortKeyType = AttributeType.S
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :a or $sortKey = :b",
            expressionAttributeValues = mapOf(":a" to atS("a"), ":b" to atS("b"))
        )
    }

    @Test
    fun `test fail key condition expression uses non-existent value`() = queryFailed<NoSuchElementException> { // TODO
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v"
        )
    }

    @Test
    fun `test fail filter expression uses non-existent value`() = queryFailed<NoSuchElementException> { // TODO
        query = query(
            tableName = tableName,
            keyConditionExpression = "$partKey = :v",
            filterExpression = "a = :a",
            expressionAttributeValues = mapOf(":v" to atS("a"))
        )
    }

    @Test
    fun `test fail invalid limit`() {
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditionExpression = "$partKey = :v",
                expressionAttributeValues = mapOf(":v" to atS("val")),
                limit = 0
            )
        }
        queryFailed<DynamoDbException> {
            query = query(
                tableName = tableName,
                keyConditionExpression = "$partKey = :v",
                expressionAttributeValues = mapOf(":v" to atS("val")),
                limit = -10
            )
        }
    }

    @Test
    fun `test invalid name of local secondary index`() = queryFailed<DynamoDbException> {
        localSecondaryIndexes = listOf(
            localSecondaryIndex("hi", ProjectionType.KEYS_ONLY, emptyList(), "wow") to AttributeType.S
        )
        query = query(
            tableName = tableName,
            indexName = "other",
            keyConditionExpression = "$partKey = :val",
            expressionAttributeValues = mapOf(":val" to atS("kek"))
        )
    }

    /*****************************************************************************************************************/
    /**
     * `Query` and `Scan` share most of the code. So we don't have to test all these things for the scan operation.
     * There is only a simple test for `Scan`.
     */

    @Test
    fun `test simple scan`() = testScan {
        partKeyType = AttributeType.S
        sortKeyType = AttributeType.S
        items = listOf(
            mapOf(partKey to atS("one"), sortKey to atS("123 hellos"), "a" to atN("kek")),
            mapOf(partKey to atS("one"), sortKey to atS("456"), "b" to atN("test"), "a" to atSS("one", "two")),
            mapOf(partKey to atS("two"), sortKey to atS("amazing")),
            mapOf(partKey to atS("Garfild"), sortKey to atS("spider")),
            mapOf(partKey to atS("Holland"), sortKey to atS("man"))
        )
        scan = ScanRequest.builder()
            .tableName(tableName)
            .projectionExpression("$partKey, $sortKey, a")
            .limit(100)
            .build()
        expected = ExpectedItems(
            items = items.map { item -> item.filterKeys { it != "b" } },
            scannedCount = 5,
            ignoreOrder = true
        )
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
