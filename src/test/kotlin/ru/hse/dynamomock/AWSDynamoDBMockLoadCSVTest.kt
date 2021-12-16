package ru.hse.dynamomock

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isFailure
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
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
            "column1" to AttributeValue.builder().s("homecoming").build(),
            "column3" to AttributeValue.builder().s("2017-07-06").build(),
            "column2" to AttributeValue.builder().n("1").build(),
            "column4" to AttributeValue.builder().bool(true).build(),
            "column5" to AttributeValue.builder().nul(false).build()
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
            "column1" to AttributeValue.builder().s("far from home").build(),
            "column3" to AttributeValue.builder().s("2019-07-04").build(),
            "column2" to AttributeValue.builder().n("2").build(),
            "column4" to AttributeValue.builder().bool(false).build(),
            "column5" to AttributeValue.builder().nul(true).build()
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
            "column1" to AttributeValue.builder().s("captain america").build(),
            "column2" to AttributeValue.builder()
                .ss(listOf("the first avenger", "the winter soldier", "civil war"))
                .build(),
            "column3" to AttributeValue.builder()
                .ns(listOf("1", "2", "3"))
                .build()
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
            "column1" to AttributeValue.builder().s("something").build(),
            "column2" to AttributeValue.builder()
                .l(
                    listOf(
                        AttributeValue.builder().s("Cookies").build(),
                        AttributeValue.builder().s("Coffee").build(),
                        AttributeValue.builder().n("3.14159").build()
                    )
                )
                .build(),
            "column3" to AttributeValue.builder()
                .m(
                    mapOf(
                        "Name" to AttributeValue.builder().s("Joe").build(),
                        "Age" to AttributeValue.builder().n("35").build()
                    )
                )
                .build()
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