package ru.hse.dynamomock

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isFailure
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import kotlin.test.assertEquals

internal class AWSDynamoDBMockLoadCSVTest : AWSDynamoDBMockTest() {
    private val filesLocation = "src/test/resources/load-items/"

    @Test
    fun `empty file`() {
        assertThat {
            mock.loadCSV(filesLocation + "empty.csv", "whatever")
        }.isFailure().hasMessage("The file is empty")
    }

    @Test
    fun `inputStream test`() {
        val inputStream = AWSDynamoDBMockLoadCSVTest::class.java.getResourceAsStream("/load-items/empty.csv")
        checkNotNull(inputStream)
        assertThat {
            mock.loadCSV(inputStream, "whatever")
        }.isFailure().hasMessage("The file is empty")
    }

    @Test
    fun `wrong value format`() {
        assertThat {
            mock.loadCSV(filesLocation + "wrong-1-value-format.csv", "whatever")
        }.isFailure().hasMessage("Wrong value format. Use <column_name>|<type>")
    }

    @Test
    fun `wrong type format`() {
        assertThat {
            mock.loadCSV(filesLocation + "wrong-2-type-format.csv", "whatever")
        }.isFailure().hasMessage("Function loadItems supports all types except B, BS")
    }

    @Test
    fun `simple load items`() {
        // create table
        val tableName = "testTable"
        val attributeDefinitions = listOf<AttributeDefinition>(
            AttributeDefinition.builder()
                .attributeName("column1")
                .attributeType("S")
                .build(),
            AttributeDefinition.builder()
                .attributeName("column3")
                .attributeType("S")
                .build()
        )
        val createTableRequest = CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(attributeDefinitions)
            .keySchema(
                listOf(
                    KeySchemaElement.builder()
                        .attributeName("column1")
                        .keyType("HASH")
                        .build(),
                    KeySchemaElement.builder()
                        .attributeName("column3")
                        .keyType("RANGE")
                        .build()
                )
            )
            .build()
        mock.createTable(createTableRequest)

        mock.loadCSV(filesLocation + "basic.csv", tableName)

        val item = mapOf(
            "column1" to stringAV("homecoming"),
            "column3" to stringAV("2017-07-06"),
            "column2" to numAV("1"),
            "column4" to boolAV(true),
            "column5" to nulAV(false)
        )

        val response = mock.getItem(
            GetItemRequest.builder()
                .tableName(tableName)
                .attributesToGet(item.keys)
                .key(item.filter { it.key == "column1" || it.key == "column3" })
                .build()
        )
        assertEquals(item, response.item())

        val item2 = mapOf(
            "column1" to stringAV("far from home"),
            "column3" to stringAV("2019-07-04"),
            "column2" to numAV("2"),
            "column4" to boolAV(false),
            "column5" to nulAV(true)
        )

        val response2 = mock.getItem(
            GetItemRequest.builder()
                .tableName(tableName)
                .attributesToGet(item2.keys)
                .key(item2.filter { it.key == "column1" || it.key == "column3" })
                .build()
        )
        assertEquals(item2, response2.item())
    }

    @Test
    fun `load items with ns and ss types`() {
        // create table
        val tableName = "testTable"
        val attributeDefinitions = listOf<AttributeDefinition>(
            AttributeDefinition.builder()
                .attributeName("column1")
                .attributeType("S")
                .build()
        )
        val createTableRequest = CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(attributeDefinitions)
            .keySchema(
                listOf(
                    KeySchemaElement.builder()
                        .attributeName("column1")
                        .keyType("HASH")
                        .build()
                )
            )
            .build()
        mock.createTable(createTableRequest)

        mock.loadCSV(filesLocation + "ss-ns-types.csv", tableName)

        val item = mapOf(
            "column1" to stringAV("captain america"),
            "column2" to stringListAV(listOf("the first avenger", "the winter soldier", "civil war")),
            "column3" to numListAV(listOf("1", "2", "3"))
        )

        val response = mock.getItem(
            GetItemRequest.builder()
                .tableName(tableName)
                .attributesToGet(item.keys)
                .key(item.filter { it.key == "column1" })
                .build()
        )
        assertEquals(item, response.item())
    }

    @Test
    fun `load l item`() {
        val tableName = "testTable"
        val attributeDefinitions = listOf<AttributeDefinition>(
            AttributeDefinition.builder()
                .attributeName("column1")
                .attributeType("S")
                .build()
        )
        val createTableRequest = CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(attributeDefinitions)
            .keySchema(
                listOf(
                    KeySchemaElement.builder()
                        .attributeName("column1")
                        .keyType("HASH")
                        .build()
                )
            )
            .build()
        mock.createTable(createTableRequest)

        mock.loadCSV(filesLocation + "l-m-types.csv", tableName)

        val item = mapOf(
            "column1" to stringAV("something"),
            "column2" to listAV(
                listOf(
                    stringAV("Cookies"),
                    stringAV("Coffee"),
                    numAV("3.14159")
                )
            ),
            "column3" to mapAV(
                mapOf(
                    "Name" to stringAV("Joe"),
                    "Age" to numAV("35")
                )
            )
        )

        val response = mock.getItem(
            GetItemRequest.builder()
                .tableName(tableName)
                .attributesToGet(item.keys)
                .key(item.filter { it.key == "column1" })
                .build()
        )
        assertEquals(item, response.item())

    }

}