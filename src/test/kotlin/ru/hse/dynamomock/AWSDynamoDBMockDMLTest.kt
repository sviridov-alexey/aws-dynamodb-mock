package ru.hse.dynamomock

import assertk.all
import assertk.assertThat
import assertk.assertions.hasClass
import assertk.assertions.isFailure
import assertk.assertions.messageContains
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

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
            partitionKeyName to "value1".asDynamoStringValue(),
            sortKeyName to "1".asDynamoNumValue(),
            "column2" to "123.367".asDynamoNumValue()
        )

        val item2 = mapOf(
            partitionKeyName to "key2".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue(),
            "column4" to listOf("we", "are", "strings").asDynamoStrListValue()
        )

        val item3 = mapOf(
            partitionKeyName to "ssssnake".asDynamoStringValue(),
            sortKeyName to "30".asDynamoNumValue(),
            "column5" to "not number 5".asDynamoStringValue(),
            "column4" to listOf("67", "2", "0.5").asDynamoNumListValue()
        )

        @JvmStatic
        fun items(): List<Arguments> {
            val arr = ByteArray(20)
            val b = SdkBytes.fromByteArray(Random.nextBytes(arr))
            val mapAttributeValue = mapOf(
                "kuku" to "ha".asDynamoStringValue(),
                "hseee" to ":)".asDynamoStringValue(),
                "scary" to "666".asDynamoNumValue()
            )

            return listOf(
                Arguments.of(item1),
                Arguments.of(item2),
                Arguments.of(item3),
                Arguments.of(
                    mapOf(
                        partitionKeyName to "key2".asDynamoStringValue(),
                        sortKeyName to "2".asDynamoNumValue(),
                        "column3" to b.asDynamoValue(),
                        "column4" to listOf(b).asDynamoValue(),
                        "column5" to true.asDynamoBoolValue(),
                        "column6" to true.asDynamoNulValue(),
                        "listColumn" to mapAttributeValue.values.asDynamoValue(),
                        "mapColumn" to mapAttributeValue.asDynamoValue(),
                        "ns" to listOf("100", "101").asDynamoNumListValue(),
                    )
                )
            )
        }
    }

    private val defaultItem = mutableMapOf(
        partitionKeyName to "value1".asDynamoStringValue(),
        sortKeyName to "1".asDynamoNumValue(),
        "column3" to "i am string".asDynamoStringValue(),
        "column10" to "87654".asDynamoStringValue()
    )

    private fun makeCreateTableRequest(name: String): CreateTableRequest {
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
        return CreateTableRequest.builder()
            .tableName(name)
            .attributeDefinitions(attributeDefinitions)
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10).writeCapacityUnits(5).build())
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
    }

    @BeforeEach
    fun createTableForDynamo() {
        client.createTable(makeCreateTableRequest(tableName))
    }

    @AfterEach
    fun dropDynamoTable() {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
    }

    @BeforeEach
    fun createTable() {
        mock.createTable(makeCreateTableRequest(tableName))
    }

    private fun createTable(name: String) {
        mock.createTable(makeCreateTableRequest(name))
        client.createTable(makeCreateTableRequest(name))
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test empty put item response`(item: Map<String, AttributeValue>) {
        val putItemRequest = putItemRequestBuilder(tableName, item)
        val mockResponse = mock.putItem(putItemRequest)
        val dynamoResponse = client.putItem(putItemRequest)
        assertTrue(mockResponse.attributes().isEmpty())
        assertTrue(dynamoResponse.attributes().isEmpty())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test attributes of put item response`(item: Map<String, AttributeValue>) {
        val putItemRequest = putItemRequestBuilder(tableName, item, ReturnValue.ALL_OLD)
        val mockResponse = mock.putItem(putItemRequest)
        val dynamoResponse = client.putItem(putItemRequest)
        assertTrue(mockResponse.attributes().isEmpty())
        assertTrue(dynamoResponse.attributes().isEmpty())

        val mockResponse2 = mock.putItem(putItemRequest)
        val dynamoResponse2 = client.putItem(putItemRequest)
        assertEquals(item, mockResponse2.attributes())
        compareItems(item, dynamoResponse2.attributes())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test put and get item combo`(item: Map<String, AttributeValue>) {
        // put request
        val putItemRequest = putItemRequestBuilder(tableName, item)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)

        // check item exists with get item
        val getItemRequest = getItemRequestBuilder(
            tableName,
            item.keys.toList(),
            keys
        )
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)
        assertEquals(item, mockResponse.item())
        compareItems(item, dynamoResponse.item())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test attributesToGet in get item`(item: Map<String, AttributeValue>) {
        // put request
        val attributeName = item.map { it.key }[0]
        val attributeName2 = item.map { it.key }[item.size - 1]
        val attributesFromItem = mapOf(
            attributeName to item[attributeName]!!,
            attributeName2 to item[attributeName2]!!,
        )
        val putItemRequest = putItemRequestBuilder(tableName, item)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        // check with get
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)
        val getItemRequest = getItemRequestBuilder(
            tableName,
            listOf(attributeName, attributeName2),
            keys
        )
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)
        assertEquals(attributesFromItem, mockResponse.item())
        compareItems(attributesFromItem, dynamoResponse.item())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test get not existing item`(item: Map<String, AttributeValue>) {
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)
        val getItemRequest = getItemRequestBuilder(
            tableName,
            item.keys.toList(),
            keys
        )
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)
        assertTrue(mockResponse.item().isEmpty())
        assertTrue(dynamoResponse.item().isEmpty())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test delete item`(item: Map<String, AttributeValue>) {
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)

        val putItemRequest = putItemRequestBuilder(tableName, item)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, item.keys.toList(), keys)
        val mockGetResponse = mock.getItem(getItemRequest)
        val dynamoGetResponse = client.getItem(getItemRequest)

        assertTrue(mockGetResponse.item().isNotEmpty())
        assertTrue(dynamoGetResponse.item().isNotEmpty())

        val deleteItemRequest = deleteItemRequestBuilder(tableName, keys)
        mock.deleteItem(deleteItemRequest)
        client.deleteItem(deleteItemRequest)

        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)
        assertTrue(mockResponse.item().isEmpty())
        assertTrue(dynamoResponse.item().isEmpty())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test attributes of delete item response`(item: Map<String, AttributeValue>) {
        val keys = keysFromItem(item, partitionKeyName, sortKeyName)
        val deleteItemRequest = deleteItemRequestBuilder(tableName, keys, ReturnValue.ALL_OLD)
        assertTrue(
            mock.deleteItem(deleteItemRequest).attributes().isEmpty() &&
                client.deleteItem(deleteItemRequest).attributes().isEmpty()
        )
        val putItemRequest = putItemRequestBuilder(tableName, item)

        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val mockResponse = mock.deleteItem(deleteItemRequest)
        val dynamoResponse = client.deleteItem(deleteItemRequest)

        assertEquals(item, mockResponse.attributes())
        compareItems(item, dynamoResponse.attributes())
    }

    @ParameterizedTest
    @MethodSource("items")
    fun `test throws when wrong returnValues`(item: Map<String, AttributeValue>) {
        val putItemRequest = putItemRequestBuilder(tableName, item, ReturnValue.UPDATED_NEW)
        val errorMessage = "Return values set to invalid value"

        assertThat {
            mock.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test wrong key name`() {
        val item = mapOf(
            partitionKeyName + "1" to "key2".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue(),
            "column4" to listOf("we", "are", "strings").asDynamoStrListValue()
        )
        val putItemRequest = putItemRequestBuilder(tableName, item)
        val errorMessage = "One of the required keys was not given a value"

        assertThat {
            mock.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test wrong key type`() {
        val item = mapOf(
            partitionKeyName to "1234".asDynamoNumValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue(),
            "column4" to listOf("we", "are", "strings").asDynamoStrListValue()
        )
        val putItemRequest = putItemRequestBuilder(tableName, item)
        val errorMessage = "One or more parameter values were invalid: Type mismatch for key"

        assertThat {
            mock.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test more keys than it should be`() {
        val item = mapOf(
            partitionKeyName to "key2".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue(),
        )
        val getItemRequest = getItemRequestBuilder(tableName, item.keys.toList(), item)
        val errorMessage = "The number of conditions on the keys is invalid"

        assertThat {
            mock.getItem(getItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.getItem(getItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test key type not from b, n, s`() {
        val item = mapOf(
            partitionKeyName to listOf("key2").asDynamoStrListValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue(),
            "column4" to listOf("we", "are", "strings").asDynamoStrListValue()
        )
        val putItemRequest = putItemRequestBuilder(tableName, item)
        val errorMessage = "Invalid attribute value type"

        assertThat {
            mock.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test wrong table name`() {
        val item = mapOf(
            partitionKeyName to "key2".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue(),
            "column4" to listOf("we", "are", "strings").asDynamoStrListValue()
        )
        val putItemRequest = putItemRequestBuilder("wrongTableName", item)
        val errorMessage = "Cannot do operations on a non-existent table"

        assertThat {
            mock.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(ResourceNotFoundException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.putItem(putItemRequest)
        }.isFailure().all {
            hasClass(ResourceNotFoundException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test different sort keys`() {
        val item = mapOf(
            partitionKeyName to "key2".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue()
        )
        val newKeys = mapOf(
            partitionKeyName to "key2".asDynamoStringValue(),
            sortKeyName to "3".asDynamoNumValue()
        )

        val putItemRequest = putItemRequestBuilder(tableName, item)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, newKeys.keys.toList(), newKeys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        assertTrue(mockResponse.item().isEmpty())
        assertTrue(dynamoResponse.item().isEmpty())
    }

    @Test
    fun `test different partition keys`() {
        val item = mapOf(
            partitionKeyName to "key2".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue(),
            "column3" to "i am string".asDynamoStringValue()
        )
        val newKeys = mapOf(
            partitionKeyName to "different key".asDynamoStringValue(),
            sortKeyName to "2".asDynamoNumValue()
        )

        val putItemRequest = putItemRequestBuilder(tableName, item)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, newKeys.keys.toList(), newKeys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        assertTrue(mockResponse.item().isEmpty())
        assertTrue(dynamoResponse.item().isEmpty())
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
                )
            )
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        val errorMessage = "Cannot do operations on a non-existent table"

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(ResourceNotFoundException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(ResourceNotFoundException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `batchWriteItem no requestItems`() {
        val requestItems = mapOf<String, List<WriteRequest>>()
        val batchWriteItemRequest1 = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        val batchWriteItemRequest2 = BatchWriteItemRequest.builder()
            .build()
        val errorMessage = "BatchWriteItem cannot have a null or no requests set"

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest1)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            mock.batchWriteItem(batchWriteItemRequest2)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.batchWriteItem(batchWriteItemRequest1)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.batchWriteItem(batchWriteItemRequest2)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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
                )
            )
        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        val errorMessage =
            "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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
        val errorMessage = "Provided list of item keys contains duplicates"

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `BatchWriteItem 25+ items in one request`() {
        val itemsList = mutableListOf<WriteRequest>()
        val str = "sTRonG"
        for (i in 0..25) {
            str.plus("1")
            val item: Map<String, AttributeValue> = mapOf(
                partitionKeyName to str.asDynamoStringValue(),
                sortKeyName to "123.3667".asDynamoNumValue(),
                "column4" to "column4".asDynamoStringValue()
            )
            itemsList.add(
                WriteRequest.builder()
                    .putRequest(
                        PutRequest.builder()
                            .item(item)
                            .build()
                    )
                    .build()
            )
        }
        val requestItems =
            mapOf<String, List<WriteRequest>>(
                tableName to itemsList
            )

        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        val errorMessage = "Too many items requested for the BatchWriteItem call"

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `BatchWriteItem 25+ items multiple tables in one request`() {
        val itemsList = mutableListOf<WriteRequest>()
        var str = "sTRonG"
        for (i in 0..13) {
            str += "1"
            val item: Map<String, AttributeValue> = mapOf(
                partitionKeyName to str.asDynamoStringValue(),
                sortKeyName to "123.3667".asDynamoNumValue(),
                "column4" to "column4".asDynamoStringValue()
            )
            itemsList.add(
                WriteRequest.builder()
                    .putRequest(
                        PutRequest.builder()
                            .item(item)
                            .build()
                    )
                    .build()
            )
        }

        createTable("testTable2")
        val itemsList2 = mutableListOf<WriteRequest>()
        var str2 = "whatever is takes"
        for (i in 0..12) {
            str2 += "1"
            val item: Map<String, AttributeValue> = mapOf(
                partitionKeyName to str2.asDynamoStringValue(),
                sortKeyName to "123.3667".asDynamoNumValue(),
            )
            itemsList2.add(
                WriteRequest.builder()
                    .putRequest(
                        PutRequest.builder()
                            .item(item)
                            .build()
                    )
                    .build()
            )
        }

        val requestItems =
            mapOf<String, List<WriteRequest>>(
                tableName to itemsList,
                "testTable2" to itemsList2
            )

        val batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build()
        val errorMessage = "Too many items requested for the BatchWriteItem call"

        assertThat {
            mock.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.batchWriteItem(batchWriteItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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
        client.batchWriteItem(batchWriteItemRequest)

        val keys1 = item1.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }
        val keys2 = item2.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }
        val keys3 = item3.entries.filter { i -> i.key == partitionKeyName || i.key == sortKeyName }
            .associate { it.key to it.value }

        val getItemRequest1 = getItemRequestBuilder(tableName, item1.keys.toList(), keys1)
        val mockResponse1 = mock.getItem(getItemRequest1)
        val dynamoResponse1 = client.getItem(getItemRequest1)

        assertEquals(item1, mockResponse1.item())
        compareItems(item1, dynamoResponse1.item())

        val getItemRequest2 = getItemRequestBuilder(tableName, item2.keys.toList(), keys2)
        val mockResponse2 = mock.getItem(getItemRequest2)
        val dynamoResponse2 = client.getItem(getItemRequest2)

        assertEquals(item2, mockResponse2.item())
        compareItems(item2, dynamoResponse2.item())

        val getItemRequest3 = getItemRequestBuilder(tableName, item3.keys.toList(), keys3)
        val mockResponse3 = mock.getItem(getItemRequest3)
        val dynamoResponse3 = client.getItem(getItemRequest3)

        assertTrue(mockResponse3.item().isEmpty())
        assertTrue(dynamoResponse3.item().isEmpty())

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
        client.batchWriteItem(batchWriteItemRequest2)

        val mockResponse12 = mock.getItem(getItemRequest1)
        val dynamoResponse12 = client.getItem(getItemRequest1)

        assertTrue(mockResponse12.item().isEmpty())
        assertTrue(dynamoResponse12.item().isEmpty())

        val mockResponse22 = mock.getItem(getItemRequest2)
        val dynamoResponse22 = client.getItem(getItemRequest2)

        assertTrue(mockResponse22.item().isEmpty())
        assertTrue(dynamoResponse22.item().isEmpty())

        val mockResponse32 = mock.getItem(getItemRequest3)
        val dynamoResponse32 = client.getItem(getItemRequest3)

        assertEquals(item3, mockResponse32.item())
        compareItems(item3, dynamoResponse32.item())
    }

    @Test
    fun `BatchWriteItem replace test`() {
        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList(), keys)
        val mockResponse1 = mock.getItem(getItemRequest)
        val dynamoResponse1 = client.getItem(getItemRequest)

        assertEquals(defaultItem, mockResponse1.item())
        compareItems(defaultItem, dynamoResponse1.item())

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
        client.batchWriteItem(batchWriteItemRequest)

        val getItemRequest2 = getItemRequestBuilder(tableName, item1.keys.toList(), keys)
        val mockResponse2 = mock.getItem(getItemRequest2)
        val dynamoResponse2 = client.getItem(getItemRequest2)

        assertEquals(item1, mockResponse2.item())
        compareItems(item1, dynamoResponse2.item())
    }

    // update item

    @Test
    fun `updateItem put (replace old)`() {
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                "column3" to attributeValueUpdateBuilder(
                    AttributeAction.PUT, "hehe".asDynamoStringValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList(), keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem["column3"] = "hehe".asDynamoStringValue()
        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `updateItem throws if value is key`() {
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val errorMessage = "Cannot update attribute partitionKey. This attribute is part of the key"
        val updateItemRequest1 = updateItemRequestBuilder(
            tableName, keys,
            mapOf(
                partitionKeyName to attributeValueUpdateBuilder(
                    AttributeAction.PUT, "hehe".asDynamoStringValue()
                )
            )
        )

        assertThat {
            mock.updateItem(updateItemRequest1)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.updateItem(updateItemRequest1)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        val updateItemRequest2 = updateItemRequestBuilder(
            tableName, keys,
            mapOf(
                partitionKeyName to attributeValueUpdateBuilder(
                    AttributeAction.DELETE
                )
            )
        )

        assertThat {
            mock.updateItem(updateItemRequest2)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.updateItem(updateItemRequest2)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        val updateItemRequest3 = updateItemRequestBuilder(
            tableName, keys,
            mapOf(
                partitionKeyName to attributeValueUpdateBuilder(
                    AttributeAction.ADD, "hehe".asDynamoStringValue()
                )
            )
        )

        assertThat {
            mock.updateItem(updateItemRequest3)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.updateItem(updateItemRequest3)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `updateItem put new attribute`() {
        val name = "newColumn"
        val value = "hehe"
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)
        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.PUT, value.asDynamoStringValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList() + name, keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem[name] = value.asDynamoStringValue()
        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `updateItem put new item`() {
        val item = keysFromItem(defaultItem, partitionKeyName, sortKeyName).toMutableMap()

        val name = "newColumn"
        val value = "hehe"

        val updateItemRequest = updateItemRequestBuilder(
            tableName, item, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.PUT, value.asDynamoStringValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, item.keys.toList() + name, item)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        item[name] = value.asDynamoStringValue()
        assertEquals(item, mockResponse.item())
        compareItems(item, dynamoResponse.item())
    }

    @Test
    fun `updateItem delete whole attribute`() {
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                "column3" to attributeValueUpdateBuilder(
                    AttributeAction.DELETE
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList(), keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem.remove("column3")
        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `updateItem delete from set`() {
        val name = "coolSet"
        defaultItem[name] = listOf("a", "b", "c").asDynamoStrListValue()

        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.DELETE,
                    listOf("a", "c").asDynamoStrListValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList(), keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem[name] = listOf("b").asDynamoStrListValue()
        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `updateItem delete from set (specifying an empty set is an error)`() {
        val name = "coolSet"
        defaultItem[name] = listOf("a", "b", "c").asDynamoStrListValue()
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)
        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.DELETE,
                    listOf<String>().asDynamoStrListValue()
                )
            )
        )
        val errorMessage = "One or more parameter values were invalid: An string set  may not be empty"

        assertThat {
            mock.updateItem(updateItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.updateItem(updateItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `updateItem delete not set error`() {
        val name = "coolNumber"
        defaultItem[name] = AttributeValue.builder()
            .n("666")
            .build()
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)
        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.DELETE,
                    AttributeValue.builder()
                        .n("666")
                        .build()
                )
            )
        )
        val errorMessage =
            "One or more parameter values were invalid: DELETE action with value is not supported for the type NUMBER"

        assertThat {
            mock.updateItem(updateItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.updateItem(updateItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `updateItem add to set`() {
        val name = "coolSet"
        defaultItem["coolSet"] = listOf("1", "2").asDynamoNumListValue()

        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.ADD,
                    listOf("3").asDynamoNumListValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList(), keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem[name] = listOf("1", "2", "3").asDynamoNumListValue()

        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `updateItem add new number attribute`() {
        val name = "newColumn"
        val value = "666"
        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.ADD, value.asDynamoNumValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList() + name, keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem[name] = value.asDynamoNumValue()
        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `updateItem add new string attribute`() {
        val name = "newColumn"
        val value = "wrongType"

        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)
        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.ADD, value.asDynamoStringValue()
                )
            )
        )
        val errorMessage = "One or more parameter values were invalid: ADD action is not supported for the type STRING"

        assertThat {
            mock.updateItem(updateItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.updateItem(updateItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `updateItem add and sum number`() {
        val name = "coolNumber"
        defaultItem[name] = AttributeValue.builder()
            .n("30")
            .build()

        val putItemRequest = putItemRequestBuilder(tableName, defaultItem)
        mock.putItem(putItemRequest)
        client.putItem(putItemRequest)

        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)

        val updateItemRequest = updateItemRequestBuilder(
            tableName, keys, mapOf(
                name to attributeValueUpdateBuilder(
                    AttributeAction.ADD, "70".asDynamoNumValue()
                )
            )
        )
        mock.updateItem(updateItemRequest)
        client.updateItem(updateItemRequest)

        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList(), keys)
        val mockResponse = mock.getItem(getItemRequest)
        val dynamoResponse = client.getItem(getItemRequest)

        defaultItem[name] = "100".asDynamoNumValue()
        assertEquals(defaultItem, mockResponse.item())
        compareItems(defaultItem, dynamoResponse.item())
    }

    @Test
    fun `test duplicate attributesToGet`() {
        val keys = keysFromItem(defaultItem, partitionKeyName, sortKeyName)
        val getItemRequest = getItemRequestBuilder(tableName, defaultItem.keys.toList() + partitionKeyName, keys)
        val errorMessage = "Cannot have two attributes with the same name"

        assertThat {
            mock.getItem(getItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.getItem(getItemRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }
}