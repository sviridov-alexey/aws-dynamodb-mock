package ru.hse.dynamomock

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement

import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import kotlin.random.Random
import kotlin.test.assertEquals

internal class AWSDynamoDBMockDMLTest : AWSDynamoDBMockTest() {

    companion object {
        const val partitionKeyName = "partitionKey"
        const val sortKeyName = "sortKey"
        const val tableName = "testTable"

        @JvmStatic
        fun items(): List<Arguments> {
            val arr = ByteArray(20)
            val b = SdkBytes.fromByteArray(Random.nextBytes(arr))
            val mapAttributeValue = mapOf<String, AttributeValue>(
                "kuku" to AttributeValue.builder().s("ha").build(),
                "hseee" to AttributeValue.builder().s(":)").build(),
                "scary" to AttributeValue.builder().n("666").build()
            )

            return listOf(
                Arguments.of(
                    mapOf(
                        partitionKeyName to AttributeValue.builder().s("value1").build(),
                        sortKeyName to AttributeValue.builder().n("1").build(),
                        "column2" to AttributeValue.builder().n("123.367").build()
                    )
                ),
                Arguments.of(
                    mapOf(
                        partitionKeyName to AttributeValue.builder().s("key2").build(),
                        sortKeyName to AttributeValue.builder().n("2").build(),
                        "column3" to AttributeValue.builder().s("i am string").build(),
                        "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
                    )
                ),
                Arguments.of(
                    mapOf(
                        partitionKeyName to AttributeValue.builder().s("key2").build(),
                        sortKeyName to AttributeValue.builder().n("2").build(),
                        "column3" to AttributeValue.builder().b(b).build(),
                        "column4" to AttributeValue.builder().bs(listOf(b, b)).build(),
                        "column5" to AttributeValue.builder().bool(true).build(),
                        "column6" to AttributeValue.builder().nul(true).build(),
                        "listColumn" to AttributeValue.builder().l(mapAttributeValue.values).build(),
                        "mapColumn" to AttributeValue.builder().m(mapAttributeValue).build(),
                        "ns" to AttributeValue.builder().ns("100", "101").build(),
                    )
                )
            )
        }
    }

    @BeforeEach
    fun createTable() {
        val attributeDefinitions = listOf<AttributeDefinition>(
            AttributeDefinition.builder()
                .attributeName(partitionKeyName)
                .attributeType("S")
                .build(),
            AttributeDefinition.builder()
                .attributeName(sortKeyName)
                .attributeType("N")
                .build()
        )
        val createTableRequest = CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(attributeDefinitions)
            .keySchema(
                listOf(
                    KeySchemaElement.builder()
                        .attributeName(partitionKeyName)
                        .keyType("HASH")
                        .build(),
                    KeySchemaElement.builder()
                        .attributeName(sortKeyName)
                        .keyType("RANGE")
                        .build()
                )
            )
            .build()
        mock.createTable(createTableRequest)
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test empty put item response`(item: Map<String, AttributeValue>) {
        val response = mock.putItem(putItemRequestBuilder(tableName, item))
        assertTrue(response.attributes().isEmpty())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test attributes of put item response`(item: Map<String, AttributeValue>) {
        val request = putItemRequestBuilder(tableName, item, ReturnValue.ALL_OLD)
        val response = mock.putItem(request)
        assertTrue(response.attributes().isEmpty())

        val response2 = mock.putItem(request)
        assertEquals(item, response2.attributes())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test put and get item combo`(item: Map<String, AttributeValue>) {
        mock.putItem(putItemRequestBuilder(tableName, item))
        val keys = item.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }

        val response = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item.keys.toList(),
                keys
            )
        )
        assertEquals(item, response.item())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test attributesToGet in get item`(item: Map<String, AttributeValue>) {
        val attributeName = item.map { it.key }[0]
        val attributeName2 = item.map { it.key }[item.size - 1]
        val attributesFromItem = mapOf(
            attributeName to item[attributeName],
            attributeName2 to item[attributeName2],
        )

        mock.putItem(putItemRequestBuilder(tableName, item))
        val keys = item.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }

        val response = mock.getItem(
            getItemRequestBuilder(
                tableName,
                listOf(attributeName, attributeName2),
                keys
            )
        )
        assertEquals(attributesFromItem, response.item())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test get not existing item`(item: Map<String, AttributeValue>) {
        val keys = item.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }

        val response = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item.keys.toList(),
                keys
            )
        )
        assertTrue(response.item().isEmpty())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test delete item`(item: Map<String, AttributeValue>) {
        val keys = item.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }

        mock.putItem(putItemRequestBuilder(tableName, item))
        mock.deleteItem(deleteItemRequestBuilder(tableName, keys))
        val response = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item.keys.toList(),
                keys
            )
        )
        assertTrue(response.item().isEmpty())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test attributes of delete item response`(item: Map<String, AttributeValue>) {
        val keys = item.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }
        assertTrue(
            mock.deleteItem(deleteItemRequestBuilder(tableName, keys, ReturnValue.ALL_OLD)).attributes()
                .isEmpty()
        )
        mock.putItem(putItemRequestBuilder(tableName, item))
        val response = mock.deleteItem(deleteItemRequestBuilder(tableName, keys, ReturnValue.ALL_OLD))

        assertEquals(item, response.attributes())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test throws when wrong returnValues`(item: Map<String, AttributeValue>) {
        val request = putItemRequestBuilder(tableName, item, ReturnValue.UPDATED_NEW)
        assertThrows<DynamoDbException> {  mock.putItem(request) }
    }

    @Test
    fun `test wrong key name`() {
        val item = mapOf(
            partitionKeyName + "1" to AttributeValue.builder().s("key2").build(),
            sortKeyName to AttributeValue.builder().n("2").build(),
            "column3" to AttributeValue.builder().s("i am string").build(),
            "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
        )
        val request = putItemRequestBuilder(tableName, item)
        assertThrows<DynamoDbException> {  mock.putItem(request) }
    }
}