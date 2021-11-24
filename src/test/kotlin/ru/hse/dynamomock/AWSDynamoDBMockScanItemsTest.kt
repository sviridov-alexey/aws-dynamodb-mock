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

internal class AWSDynamoDBMockScanItemsTest : AWSDynamoDBMockTest() {
    private val filesLocation = "src/test/resources/scan-items/"

    @Test
    fun `empty file`() {
        assertThat {
            mock.scanItemsFromCSV(filesLocation + "empty.csv", "whatever")
        }.isFailure().hasMessage("The file is empty")
    }

    @Test
    fun `wrong value format`() {
        assertThat {
            mock.scanItemsFromCSV(filesLocation + "wrong-1-value-format.csv", "whatever")
        }.isFailure().hasMessage("Wrong value format. Use <column_name>|<type>")
    }

    @Test
    fun `wrong type format`() {
        assertThat {
            mock.scanItemsFromCSV(filesLocation + "wrong-2-type-format.csv", "whatever")
        }.isFailure().hasMessage("Function scanItems supports only S, N, NS, SS, NULL, BOOL types right now")
    }

    @Test
    fun `wrong size of rows`() {
        assertThat {
            mock.scanItemsFromCSV(filesLocation + "wrong-3-size-match.csv", "whatever")
        }.isFailure().hasMessage("2 row's size doesn't match the size of first row")
        assertThat {
            mock.scanItemsFromCSV(filesLocation + "wrong-3-size-match-2.csv", "whatever")
        }.isFailure().hasMessage("3 row's size doesn't match the size of first row")
    }

    @Test
    fun `simple scan items`() {
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

        mock.scanItemsFromCSV(filesLocation + "basic.csv", tableName)

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
    fun `scan items with ns and ss types`() {
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

        mock.scanItemsFromCSV(filesLocation + "ss-ns-types.csv", tableName)

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
}