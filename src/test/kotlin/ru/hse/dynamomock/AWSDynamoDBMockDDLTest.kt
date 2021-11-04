package ru.hse.dynamomock

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

internal class AWSDynamoDBMockDDLTest : AWSDynamoDBMockTest() {
    @Test
    fun `test create table response`() {
        for (metadata in metadataPool) {
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
            assertTrue(description.creationDateTime() <= Instant.now())
        }
    }

    @Test
    fun `test create table with same name`() {
        for ((i, metadata) in metadataPool.dropLast(1).withIndex()) {
            val request = metadata.toCreateTableRequest()
            mock.createTable(request)
            assertThrows<IllegalArgumentException> {
                mock.createTable(request)
            }
            val request2 = metadataPool[i + 1].toCreateTableRequest().toBuilder().tableName(request.tableName()).build()
            assertThrows<IllegalArgumentException> {
                mock.createTable(request2)
            }
        }
    }

    @Test
    fun `test drop table`() {
        for (metadata in metadataPool) {
            val createRequest = metadata.toCreateTableRequest()
            val dropRequest = metadata.toDeleteTableRequest()
            val describeDescription = metadata.toDescribeTableRequest()
            val createDescription = mock.createTable(createRequest).tableDescription()
            val description = mock.describeTable(describeDescription).table()
            val deleteDescription = mock.deleteTable(dropRequest).tableDescription()
            assertEquals(createDescription, description)
            assertEquals(description, deleteDescription)
        }
    }

    @Test
    fun `test drop non-existent table`() {
        val request = metadataPool.last().toDeleteTableRequest()
        assertThrows<IllegalArgumentException> {
            mock.deleteTable(request)
        }
    }

    @Test
    fun `test drop table twice`() {
        for (metadata in metadataPool) {
            mock.createTable(metadata.toCreateTableRequest())
        }
        val deleteRequest = metadataPool[7].toDeleteTableRequest()
        mock.deleteTable(deleteRequest)
        assertThrows<IllegalArgumentException> {
            mock.deleteTable(deleteRequest)
        }
    }

    @Test
    fun `test describe table`() {
        for (metadata in metadataPool) {
            val description = mock.createTable(metadata.toCreateTableRequest()).tableDescription()
            assertEquals(description, mock.describeTable(metadata.toDescribeTableRequest()).table())
        }
    }

    @Test
    fun `test describe non-existent table`() {
        for (metadata in metadataPool.drop(1)) {
            mock.createTable(metadata.toCreateTableRequest())
        }
        assertThrows<IllegalArgumentException> {
            mock.describeTable(metadataPool.first().toDescribeTableRequest())
        }
    }
}
