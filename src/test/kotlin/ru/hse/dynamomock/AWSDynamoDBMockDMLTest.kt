package ru.hse.dynamomock

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isFailure
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeAction
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.PutRequest

import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.WriteRequest
import kotlin.random.Random
import kotlin.test.assertEquals

internal class AWSDynamoDBMockDMLTest : AWSDynamoDBMockTest() {

    companion object {
        const val partitionKeyName = "partitionKey"
        const val sortKeyName = "sortKey"
        const val tableName = "testTable"

        val item1 = mapOf(
            partitionKeyName to AttributeValue.builder().s("value1").build(),
            sortKeyName to AttributeValue.builder().n("1").build(),
            "column2" to AttributeValue.builder().n("123.367").build()
        )

        val item2 =  mapOf(
            partitionKeyName to AttributeValue.builder().s("key2").build(),
            sortKeyName to AttributeValue.builder().n("2").build(),
            "column3" to AttributeValue.builder().s("i am string").build(),
            "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
        )

        val item3 = mapOf(
            partitionKeyName to AttributeValue.builder().s("ssssnake").build(),
            sortKeyName to AttributeValue.builder().n("30").build(),
            "column5" to AttributeValue.builder().s("not number 5").build(),
            "column4" to AttributeValue.builder().ns(listOf("67", "2", "0.5")).build()
        )

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
                Arguments.of(item1),
                Arguments.of(item2),
                Arguments.of(item3),
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

    private fun createTable(name: String) {
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
            .tableName(name)
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

    private val defaultItem = mutableMapOf(
        partitionKeyName to stringAV("value1"),
        sortKeyName to numAV("1"),
        "column3" to stringAV("i am string"),
        "column10" to stringAV("87654")
    )

    @BeforeEach
    fun createTable() {
        createTable(tableName)
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
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)

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
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)

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
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)

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
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)

        mock.putItem(putItemRequestBuilder(tableName, item))
        val getResponse = mock.getItem(getItemRequestBuilder(tableName, item.keys.toList(), keys))
        assertTrue(getResponse.item().isNotEmpty())
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
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)
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

        assertThat {
            mock.putItem(request)
        }.isFailure().hasMessage("Return values set to invalid value")
    }

    @Test
    fun `test wrong key name`() {
        val item = mapOf(
            partitionKeyName + "1" to stringAV("key2"),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string"),
            "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
        )
        val request = putItemRequestBuilder(tableName, item)

        assertThat {
            mock.putItem(request)
        }.isFailure().hasMessage("One of the required keys was not given a value")
    }

    @Test
    fun `test wrong key type`() {
        val item = mapOf(
            partitionKeyName to numAV("1234"),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string"),
            "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
        )
        val request = putItemRequestBuilder(tableName, item)

        assertThat {
            mock.putItem(request)
        }.isFailure().hasMessage("Invalid attribute value type")
    }

    @Test
    fun `test more keys than it should be`() {
        val item = mapOf(
            partitionKeyName to stringAV("key2"),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string"),
        )
        val request = getItemRequestBuilder(tableName, item.keys.toList(), item)

        assertThat {
            mock.getItem(request)
        }.isFailure().hasMessage("The number of conditions on the keys is invalid")
    }

    @Test
    fun `test key type not from b, n, s`() {
        val item = mapOf(
            partitionKeyName to AttributeValue.builder().ss(listOf("key2")).build(),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string"),
            "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
        )
        val request = putItemRequestBuilder(tableName, item)
        assertThat {
            mock.putItem(request)
        }.isFailure().hasMessage("Member must satisfy enum value set: [B, N, S]")
    }

    @Test
    fun `test wrong table name`() {
        val item = mapOf(
            partitionKeyName to stringAV("key2"),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string"),
            "column4" to AttributeValue.builder().ss(listOf("we", "are", "strings")).build()
        )
        val request = putItemRequestBuilder("wrongTableName", item)
        assertThat {
            mock.putItem(request)
        }.isFailure().hasMessage("Cannot do operations on a non-existent table")
    }

    @Test
    fun `test different sort keys`() {
        val item = mapOf(
            partitionKeyName to stringAV("key2"),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string")
        )

        val newKeys = mapOf(
            partitionKeyName to stringAV("key2"),
            sortKeyName to numAV("3")
        )

        val request = putItemRequestBuilder(tableName, item)
        mock.putItem(request)

        val response = mock.getItem(getItemRequestBuilder(
            tableName,
            newKeys.keys.toList(),
            newKeys
        ))
        assertTrue(response.item().isEmpty())
    }

    @Test
    fun `test different partition keys`() {
        val item = mapOf(
            partitionKeyName to stringAV("key2"),
            sortKeyName to numAV("2"),
            "column3" to stringAV("i am string")
        )

        val newKeys = mapOf(
            partitionKeyName to stringAV("different key"),
            sortKeyName to numAV("2")
        )

        val request = putItemRequestBuilder(tableName, item)
        mock.putItem(request)

        val response = mock.getItem(getItemRequestBuilder(
            tableName,
            newKeys.keys.toList(),
            newKeys
        ))
        assertTrue(response.item().isEmpty())
    }

    // batchWriteItem

    @ParameterizedTest
    @MethodSource("items")
    fun `batchWriteItem wrong tableName`(item: Map<String, AttributeValue>) {
        val requestItems =
            mapOf<String, List<WriteRequest>>(
                "testTable2" to listOf(
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item)
                                .build()
                        )
                        .build()
                ))
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure().hasMessage("Cannot do operations on a non-existent table")
    }

    @Test
    fun `batchWriteItem no requestItems`() {
        val requestItems = mapOf<String, List<WriteRequest>>()
        val batchWriteItemRequest1 = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        val batchWriteItemRequest2 = BatchWriteItemRequest.builder()
            .build()
        val e1 = assertThrows<DynamoDbException> { mock.batchWriteItem(batchWriteItemRequest1) }
        assertEquals("BatchWriteItem cannot have a null or no requests set", e1.message)
        val e2 = assertThrows<DynamoDbException> { mock.batchWriteItem(batchWriteItemRequest2) }
        assertEquals("BatchWriteItem cannot have a null or no requests set", e2.message)
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `batchWriteItem putRequest and deleteRequest in one`(item: Map<String, AttributeValue>) {
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)
        val requestItems =
            mapOf<String, List<WriteRequest>>(
                "testTable" to listOf(
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item)
                                .build()
                        )
                        .deleteRequest(
                            DeleteRequest.builder()
                                .key(keys)
                                .build()
                        )
                        .build()
                ))
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure()
            .hasMessage("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `batchWriteItem duplicate items`(item: Map<String, AttributeValue>) {
        val requestItems =
            mapOf<String, List<WriteRequest>>(
                "testTable" to listOf(
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item)
                                .build()
                        )
                        .build(),
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item)
                                .build()
                        )
                        .build()
                )
            )
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure()
            .hasMessage("Provided list of item keys contains duplicates")
    }

    @Test
    fun `BatchWriteItem 25+ items in one request`() {
        val itemsList = mutableListOf<WriteRequest>()
        val str = "sTRonG"
        for (i in 0..25) {
            str.plus("1")
            val item : Map<String, AttributeValue> = mapOf(
                partitionKeyName to stringAV(str),
                sortKeyName to numAV("123.3667"),
                "column4" to stringAV("column4")
            )
            itemsList.add(WriteRequest.builder()
                .putRequest(
                    PutRequest.builder()
                        .item(item)
                        .build()
                )
                .build())
        }
        val requestItems =
            mapOf<String, List<WriteRequest>>(
                tableName to itemsList)

        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure()
            .hasMessage("Too many items requested for the BatchWriteItem call")
    }

    @Test
    fun `BatchWriteItem 25+ items multiple tables in one request`() {
        val itemsList = mutableListOf<WriteRequest>()
        var str = "sTRonG"
        for (i in 0..13) {
            str += "1"
            val item : Map<String, AttributeValue> = mapOf(
                partitionKeyName to stringAV(str),
                sortKeyName to numAV("123.3667"),
                "column4" to stringAV("column4")
            )
            itemsList.add(WriteRequest.builder()
                .putRequest(
                    PutRequest.builder()
                        .item(item)
                        .build()
                )
                .build())
        }

        createTable("testTable2")
        val itemsList2 = mutableListOf<WriteRequest>()
        var str2 = "whatever is takes"
        for (i in 0..12) {
            str2 += "1"
            val item : Map<String, AttributeValue> = mapOf(
                partitionKeyName to stringAV(str2),
                sortKeyName to numAV("123.3667"),
            )
            itemsList2.add(WriteRequest.builder()
                .putRequest(
                    PutRequest.builder()
                        .item(item)
                        .build()
                )
                .build())
        }

        val requestItems =
            mapOf<String, List<WriteRequest>>(
                tableName to itemsList,
                "testTable2" to itemsList2)

        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure()
            .hasMessage("Too many items requested for the BatchWriteItem call")
    }

    @Test
    fun `batchWriteItem test`() {
        val requestItems =
            mapOf<String, List<WriteRequest>>(
                "testTable" to listOf(
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item1)
                                .build()
                        )
                        .build(),
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item2)
                                .build()
                        )
                        .build()
                )
            )
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        mock.batchWriteItem(batchWriteItemRequest)

        val keys1 = item1.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }
        val keys2 = item2.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }
        val keys3 = item3.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item1.keys.toList(),
                keys1
            )
        )

        assertEquals(item1, response1.item())

        val response2 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item2.keys.toList(),
                keys2
            )
        )

        assertEquals(item2, response2.item())

        val response3 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item3.keys.toList(),
                keys3
            )
        )

        assertTrue(response3.item().isEmpty())

        val requestItems2 =
            mapOf<String, List<WriteRequest>>(
                "testTable" to listOf(
                    WriteRequest.builder()
                        .deleteRequest(
                            DeleteRequest.builder()
                                .key(keys1)
                                .build()
                        )
                        .build(),
                    WriteRequest.builder()
                        .deleteRequest(
                            DeleteRequest.builder()
                                .key(keys2)
                                .build()
                        )
                        .build(),
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item3)
                                .build()
                        )
                        .build()
                )
            )
        val batchWriteItemRequest2 = BatchWriteItemRequest.builder()
            .requestItems(requestItems2)
            .build()
        mock.batchWriteItem(batchWriteItemRequest2)

        val response12 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item1.keys.toList(),
                keys1
            )
        )

        assertTrue(response12.item().isEmpty())

        val response22 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item2.keys.toList(),
                keys2
            )
        )
        assertTrue(response22.item().isEmpty())

        val response32 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item3.keys.toList(),
                keys3
            )
        )

        assertEquals(item3, response32.item())
    }

    @Test
    fun `BatchWriteItem replace test`() {
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList(),
                keys
            )
        )

        assertEquals(defaultItem, response1.item())

        val requestItems =
            mapOf<String, List<WriteRequest>>(
                tableName to listOf(
                    WriteRequest.builder()
                        .putRequest(
                            PutRequest.builder()
                                .item(item1)
                                .build()
                        )
                        .build()
                )
            )
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        mock.batchWriteItem(batchWriteItemRequest)

        val response2 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item1.keys.toList(),
                keys
            )
        )

        assertEquals(item1, response2.item())
    }


    // update item

    @Test
    fun `updateItem put (replace old)`() {
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    "column3" to attributeValueUpdateBuilder(
                        AttributeAction.PUT, stringAV("hehe")
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList(),
                keys
            )
        )

        defaultItem["column3"] = stringAV("hehe")
        assertEquals(defaultItem, response1.item())
    }

    @Test
    fun `updateItem throws if value is key`() {
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        assertThrows<DynamoDbException> {
            mock.updateItem(
                updateItemRequestBuilder(
                    tableName, keys,
                    mapOf(
                        partitionKeyName to attributeValueUpdateBuilder(
                            AttributeAction.PUT, stringAV("hehe")
                        )
                    )
                )
            )
        }

        assertThrows<DynamoDbException> {
            mock.updateItem(
                updateItemRequestBuilder(
                    tableName, keys,
                    mapOf(
                        partitionKeyName to attributeValueUpdateBuilder(
                            AttributeAction.DELETE
                        )
                    )
                )
            )
        }

        assertThrows<DynamoDbException> {
            mock.updateItem(
                updateItemRequestBuilder(
                    tableName, keys,
                    mapOf(
                        partitionKeyName to attributeValueUpdateBuilder(
                            AttributeAction.ADD, stringAV("hehe")
                        )
                    )
                )
            )
        }

    }

    @Test
    fun `updateItem put new attribute`() {
        val name = "newColumn"
        val value = "hehe"
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    name to attributeValueUpdateBuilder(
                        AttributeAction.PUT, stringAV(value)
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList() + name,
                keys
            )
        )

        defaultItem[name] = stringAV(value)
        assertEquals(defaultItem, response1.item())
    }

    @Test
    fun `updateItem put new item`() {
        val item = keysFromItem(defaultItem, partitionKeyName, sortKeyName).toMutableMap()

        val name = "newColumn"
        val value = "hehe"

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, item, mapOf(
                    name to attributeValueUpdateBuilder(
                        AttributeAction.PUT, stringAV(value)
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                item.keys.toList() + name,
                item
            )
        )

        item[name] = stringAV(value)
        assertEquals(item, response1.item())
    }

    @Test
    fun `updateItem delete whole attribute`() {
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    "column3" to attributeValueUpdateBuilder(
                        AttributeAction.DELETE
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList(),
                keys
            )
        )

        defaultItem.remove("column3")
        assertEquals(defaultItem, response1.item())
    }

    @Test
    fun `updateItem delete from set`() {
        val name = "coolSet"
        defaultItem[name] = AttributeValue.builder()
            .ss("a", "b", "c")
            .build()
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    name to attributeValueUpdateBuilder(
                        AttributeAction.DELETE,
                        AttributeValue.builder()
                            .ss("a", "c")
                            .build()
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList(),
                keys
            )
        )
        defaultItem[name] = AttributeValue.builder()
            .ss("b")
            .build()

        assertEquals(defaultItem, response1.item())
    }

    @Test
    fun `updateItem delete from set (specifying an empty set is an error)`() {
        val name = "coolSet"
        defaultItem[name] = AttributeValue.builder()
            .ss("a", "b", "c")
            .build()
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        assertThrows<DynamoDbException> {
            mock.updateItem(
                updateItemRequestBuilder(
                    tableName, keys, mapOf(
                        name to attributeValueUpdateBuilder(
                            AttributeAction.DELETE,
                            AttributeValue.builder()
                                .ss(listOf())
                                .build()
                        )
                    )
                )
            )
        }

    }

    @Test
    fun `updateItem delete not set error`() {
        val name = "coolNumber"
        defaultItem[name] = AttributeValue.builder()
            .n("666")
            .build()
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        assertThrows<DynamoDbException> {
            mock.updateItem(
                updateItemRequestBuilder(
                    tableName, keys, mapOf(
                        name to attributeValueUpdateBuilder(
                            AttributeAction.DELETE,
                            AttributeValue.builder()
                                .n("666")
                                .build()
                        )
                    )
                )
            )
        }

    }


    @Test
    fun `updateItem add to set`() {
        val name = "coolSet"
        defaultItem["coolSet"] = AttributeValue.builder()
            .ns("1", "2")
            .build()
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    name to attributeValueUpdateBuilder(
                        AttributeAction.ADD,
                        AttributeValue.builder()
                            .ns("3")
                            .build()
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList(),
                keys
            )
        )
        defaultItem[name] = AttributeValue.builder()
            .ns("1", "2", "3")
            .build()

        assertEquals(defaultItem, response1.item())
    }

    @Test
    fun `updateItem add new number attribute`() {
        val name = "newColumn"
        val value = "666"
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    name to attributeValueUpdateBuilder(
                        AttributeAction.ADD, numAV(value)
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList() + name,
                keys
            )
        )

        defaultItem[name] = numAV(value)
        assertEquals(defaultItem, response1.item())
    }

    @Test
    fun `updateItem add new string attribute`() {
        val name = "newColumn"
        val value = "wrongType"
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        assertThrows<DynamoDbException> {
            mock.updateItem(
                updateItemRequestBuilder(
                    tableName, keys, mapOf(
                        name to attributeValueUpdateBuilder(
                            AttributeAction.ADD, stringAV(value)
                        )
                    )
                )

            )
        }
    }

    @Test
    fun `updateItem add and sum number`() {
        val name = "coolNumber"
        defaultItem[name] = AttributeValue.builder()
            .n("30")
            .build()
        mock.putItem(putItemRequestBuilder(tableName, defaultItem))

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        mock.updateItem(
            updateItemRequestBuilder(
                tableName, keys, mapOf(
                    name to attributeValueUpdateBuilder(
                        AttributeAction.ADD, numAV("70")
                    )
                )
            )
        )

        val response1 = mock.getItem(
            getItemRequestBuilder(
                tableName,
                defaultItem.keys.toList() + name,
                keys
            )
        )

        defaultItem[name] = numAV("100")
        assertEquals(defaultItem, response1.item())
    }

}