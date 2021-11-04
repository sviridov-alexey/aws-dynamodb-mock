package ru.hse.dynamomock

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

internal class AWSDynamoDBMockDDLTest : AWSDynamoDBMockTest() {
    private fun `test create table response`(metadata: TableMetadata) {
        val description = mock.createTable(metadata.toCreateTableRequest()).tableDescription()
        assertEquals(metadata.tableName, description.tableName())
        assertEquals(metadata.attributeDefinitions, description.attributeDefinitions())
        assertEquals(
            listOfNotNull(metadata.partitionKey, metadata.sortKey),
            listOfNotNull(
                description.keySchema().first { it.keyType() == KeyType.HASH }.attributeName(),
                description.keySchema().firstOrNull { it.keyType() == KeyType.RANGE }?.attributeName()
            )
        )
        assertEquals(metadata.tableStatus, TableStatus.ACTIVE)
        assertTrue(metadata.creationDateTime <= Instant.now())
    }

    @Test
    fun `test create table response`() = repeat(100) {
        `test create table response`(RandomDataGenerator.generate())
    }

    @Test
    fun `test create table with same name`() {
        val request = RandomDataGenerator.generate().toCreateTableRequest()
        mock.createTable(request)
        assertThrows<IllegalArgumentException> {
            mock.createTable(request)
        }
        val request2 = RandomDataGenerator.generate().toCreateTableRequest()
            .toBuilder().tableName(request.tableName()).build()
        assertThrows<IllegalArgumentException> {
            mock.createTable(request2)
        }
    }

    @Test
    fun `test drop table`() {
        val metadata = RandomDataGenerator.generate()
        val createRequest = metadata.toCreateTableRequest()
        val dropRequest = metadata.toDeleteTableRequest()
        repeat(100) {
            assertEquals(
                mock.createTable(createRequest).tableDescription(),
                mock.deleteTable(dropRequest).tableDescription()
            )
        }
    }

    @Test
    fun `test drop non-existent table`() {
        val request = RandomDataGenerator.generate().toDeleteTableRequest()
        assertThrows<IllegalArgumentException> {
            mock.deleteTable(request)
        }
    }

    @Test
    fun `test drop table twice`() {
        val metadata = RandomDataGenerator.generate()
        val createRequest = metadata.toCreateTableRequest()
        val deleteRequest = metadata.toDeleteTableRequest()
        mock.createTable(createRequest)
        mock.deleteTable(deleteRequest)
        assertThrows<IllegalArgumentException> {
            mock.deleteTable(deleteRequest)
        }
    }
}