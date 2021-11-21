package ru.hse.dynamomock

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import java.io.IOException
import kotlin.test.assertEquals

internal class AWSDynamoDBMockScanItemsTest : AWSDynamoDBMockTest() {
    private val filesLocation = "src/test/resources/scan-items/"

    @Test
    fun `empty file`() {
        val e = assertThrows<IOException> { mock.scanItemsFromCSV(filesLocation + "empty.csv", "whatever") }
        assertEquals("The file is empty", e.message)
    }

    @Test
    fun `wrong value format`() {
        val e = assertThrows<IOException> { mock.scanItemsFromCSV(filesLocation + "wrong-1-value-format.csv", "whatever") }
        assertEquals("Wrong value format. Use <column_name>|<type>", e.message)
    }

    @Test
    fun `wrong type format`() {
        val e = assertThrows<IOException> { mock.scanItemsFromCSV(filesLocation + "wrong-2-type-format.csv", "whatever") }
        assertEquals("ScanItems supports only S, N, NS or SS types right now", e.message)
    }

    @Test
    fun `wrong size of rows`() {
        val e = assertThrows<IOException> { mock.scanItemsFromCSV(filesLocation + "wrong-3-size-match.csv", "whatever") }
        assertEquals("2 row's size doesn't match the size of first row", e.message)
        val e2 = assertThrows<IOException> { mock.scanItemsFromCSV(filesLocation + "wrong-3-size-match-2.csv", "whatever") }
        assertEquals("3 row's size doesn't match the size of first row", e2.message)
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
            "column2" to AttributeValue.builder().n("1").build()
        )

        val response = mock.getItem(
            GetItemRequest.builder()
                .tableName(tableName)
                .attributesToGet(item.keys)
                .key(item.filter { it.key != "column2" })
                .build()
        )
        assertEquals(item, response.item())
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