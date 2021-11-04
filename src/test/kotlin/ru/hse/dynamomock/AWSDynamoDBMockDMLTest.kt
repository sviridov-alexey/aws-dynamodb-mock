package ru.hse.dynamomock

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import kotlin.test.assertEquals

internal class AWSDynamoDBMockDMLTest : AWSDynamoDBMockTest() {

    @Test
    fun `test empty put item response`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()
        val item = RandomDataGenerator.generateItem(description.attributeDefinitions())
        repeat(10) {
            val response = mock.putItem(putItemRequestBuilder(description.tableName(), item))
            assertTrue(response.attributes().isEmpty())
        }

    }

    @Test
    fun `test attributes of put item response`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()

        repeat(10) {
            val item = RandomDataGenerator.generateItem(description.attributeDefinitions())

            val request = putItemRequestBuilder(description.tableName(), item, ReturnValue.ALL_OLD)
            val response = mock.putItem(request)
            assertTrue(response.attributes().isEmpty())

            val response2 = mock.putItem(request)
            assertEquals(item, response2.attributes())
        }
    }

    @Test
    fun `test put and get item combo`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()

        repeat(10) {
            val item = RandomDataGenerator.generateItem(description.attributeDefinitions())
            mock.putItem(putItemRequestBuilder(description.tableName(), item))
            val keys = item.entries.filter { i -> i.key in description.keySchema().map { it.attributeName() } }.associate { it.key to it.value }

            val response = mock.getItem(getItemRequestBuilder(
                description.tableName(),
                description.attributeDefinitions().map { it.attributeName() },
                keys
            ))
            assertEquals(item, response.item())
        }
    }

    @Test
    fun `test attributesToGet in get item`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()

        repeat(10) {
            val item = RandomDataGenerator.generateItem(description.attributeDefinitions())
            val attributeName = description.attributeDefinitions().random().attributeName()
            val attributesFromItem = mapOf(attributeName to item[attributeName])

            mock.putItem(putItemRequestBuilder(description.tableName(), item))
            val keys = item.entries.filter { i -> i.key in description.keySchema().map { it.attributeName() } }.associate { it.key to it.value }

            val response = mock.getItem(getItemRequestBuilder(
                description.tableName(),
                listOf(attributeName),
                keys
            ))
            assertEquals(attributesFromItem, response.item())
        }
    }

    @Test
    fun `test get not existing item`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()

        repeat(10) {
            val item = RandomDataGenerator.generateItem(description.attributeDefinitions())
            val keys = item.entries.filter { i -> i.key in description.keySchema().map { it.attributeName() } }.associate { it.key to it.value }

            val response = mock.getItem(getItemRequestBuilder(
                description.tableName(),
                description.attributeDefinitions().map { it.attributeName() },
                keys
            ))
            assertTrue(response.item().isEmpty())
        }
    }

    @Test
    fun `test delete item`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()

        repeat(10) {
            val item = RandomDataGenerator.generateItem(description.attributeDefinitions())
            val keys = item.entries.filter { i -> i.key in description.keySchema().map { it.attributeName() } }.associate { it.key to it.value }
            mock.putItem(putItemRequestBuilder(description.tableName(), item))
            mock.deleteItem(deleteItemRequestBuilder(description.tableName(), keys))
            val response = mock.getItem(getItemRequestBuilder(
                description.tableName(),
                description.attributeDefinitions().map { it.attributeName() },
                keys
            ))
            assertTrue(response.item().isEmpty())
        }
    }

    @Test
    fun `test attributes of delete item response`() {
        val description = mock.createTable(RandomDataGenerator.generate().toCreateTableRequest()).tableDescription()

        repeat(10) {
            val item = RandomDataGenerator.generateItem(description.attributeDefinitions())
            val keys = item.entries.filter { i -> i.key in description.keySchema().map { it.attributeName() } }.associate { it.key to it.value }
            assertTrue(mock.deleteItem(deleteItemRequestBuilder(description.tableName(), keys, ReturnValue.ALL_OLD)).attributes().isEmpty())
            mock.putItem(putItemRequestBuilder(description.tableName(), item))
            val response = mock.deleteItem(deleteItemRequestBuilder(description.tableName(), keys, ReturnValue.ALL_OLD))

            assertEquals(item, response.attributes())
        }
    }
}