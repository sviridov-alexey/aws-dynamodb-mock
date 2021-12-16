package ru.hse.dynamomock

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isFailure
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.hse.dynamomock.model.TableMetadata
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
            assertThrows<DynamoDbException> {
                mock.createTable(request)
            }
            val request2 = metadataPool[i + 1].toCreateTableRequest().toBuilder().tableName(request.tableName()).build()
            assertThrows<DynamoDbException> {
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
        assertThrows<DynamoDbException> {
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
        assertThrows<DynamoDbException> {
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
        assertThrows<DynamoDbException> {
            mock.describeTable(metadataPool.first().toDescribeTableRequest())
        }
    }

    @Test
    fun `test more indexes than it should be`() {
        assertThat {
            mock.createTable(createTableMetadata("testTable", 0, 1, Instant.now(), 3).toCreateTableRequest())
        }.isFailure().hasMessage("Cannot have more than 5 local secondary indexes per table")
    }

    @Test
    fun `test same indices name`() {
        val attributeDefinitions = listOf(
            AttributeDefinition.builder()
                .attributeName("column1")
                .attributeType("S")
                .build(),
            AttributeDefinition.builder()
                .attributeName("column2")
                .attributeType("S")
                .build(),
            AttributeDefinition.builder()
                .attributeName("column3")
                .attributeType("N")
                .build()
        )
        val sameLSI = LocalSecondaryIndex.builder()
            .indexName("sameIndex")
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
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
        assertThat {
            mock.createTable(
                CreateTableRequest.builder()
                    .tableName("testTable")
                    .attributeDefinitions(attributeDefinitions)
                    .keySchema(
                        listOf(
                            KeySchemaElement.builder()
                                .attributeName("column1")
                                .keyType("HASH")
                                .build(),
                            KeySchemaElement.builder()
                                .attributeName("column2")
                                .keyType("RANGE")
                                .build()
                        )
                    )
                    .localSecondaryIndexes(listOf(
                        sameLSI,
                        sameLSI
                    ))
                    .build()
                )
        }.isFailure().hasMessage("Two local secondary indices have the same name")
    }

    @Test
    fun `test wrong index name`() {
        assertThat {
            mock.createTable(createTableMetadata("testTable", 0, 1, Instant.now(), 5).toCreateTableRequest())
        }.isFailure().hasMessage(
            """
                Invalid table/index name.  Table/index names must be between 3 and 255 characters long, 
                and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.
            """.trimIndent()
        )
    }

    @Test
    fun `test no projection in index`() {
        assertThat {
            mock.createTable(createTableMetadata("testTable", 0, 1, Instant.now(), 7).toCreateTableRequest())
        }.isFailure().hasMessage("Indexes must have a projection specified")
    }

    @Test
    fun `test no keySchema in index`() {
        assertThat {
            mock.createTable(createTableMetadata("testTable", 0, 1, Instant.now(), 8).toCreateTableRequest())
        }.isFailure().hasMessage("No defined key schema.  A key schema containing at least a hash key must be defined for all tables")
    }
}
