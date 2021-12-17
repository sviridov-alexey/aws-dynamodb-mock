package ru.hse.dynamomock

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isFailure
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

internal class AWSDynamoDBMockDDLTest : AWSDynamoDBMockTest() {
    private fun attributeToKey(attributeDefinition: AttributeDefinition, keyType: KeyType): KeySchemaElement {
        return KeySchemaElement.builder()
            .attributeName(attributeDefinition.attributeName())
            .keyType(keyType)
            .build()
    }

    private fun getAttributeDefinitions(len: Int): List<AttributeDefinition> {
        val definitions = mutableListOf<AttributeDefinition>()
        for (i in 1..len) {
            definitions.add(
                AttributeDefinition.builder()
                    .attributeName("column$i")
                    .attributeType("S")
                    .build()
            )
        }
        return definitions
    }

    private fun buildLocalSecondaryIndex(
        indexName: String?, projectionType: ProjectionType?,
        sortKeyName: String? = null
    ) =
        LocalSecondaryIndex.builder()
            .indexName(indexName)
            .projection(Projection.builder().projectionType(projectionType).build())
            .keySchema(sortKeyName?.let {
                listOf(
                    KeySchemaElement.builder().attributeName("column1")
                        .keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName(sortKeyName)
                        .keyType(KeyType.RANGE).build(),
                )
            }
            )
            .build()

    private fun createTableWithIndex(indexes: List<LocalSecondaryIndex>): CreateTableRequest {
        val numOfIndexes = indexes.size
        val attributeDefinitions = getAttributeDefinitions(2 + numOfIndexes)
        val hashKey = attributeToKey(attributeDefinitions[0], KeyType.HASH)
        val keySchema = listOf(hashKey, attributeToKey(attributeDefinitions[1], KeyType.RANGE))

        return CreateTableRequest.builder()
            .tableName("testTable")
            .attributeDefinitions(attributeDefinitions)
            .keySchema(keySchema)
            .localSecondaryIndexes(indexes)
            .build()
    }

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
        val manyLSI = buildLocalSecondaryIndex("manyIndex", ProjectionType.ALL, "column2")
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(manyLSI, manyLSI, manyLSI, manyLSI, manyLSI, manyLSI))
            )
        }.isFailure().hasMessage("Cannot have more than 5 local secondary indexes per table")
    }

    @Test
    fun `test same indices name`() {
        val sameLSI = buildLocalSecondaryIndex("sameIndex", ProjectionType.ALL, "column2")
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(sameLSI, sameLSI))
            )
        }.isFailure().hasMessage("Two local secondary indices have the same name")
    }

    @Test
    fun `test wrong index name`() {
        val lsiWithWrongName = buildLocalSecondaryIndex("wrongName,,,,", ProjectionType.ALL, "column2")
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsiWithWrongName))
            )
        }.isFailure().hasMessage(
            """
                Invalid table/index name.  Table/index names must be between 3 and 255 characters long, 
                and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'
            """.trimIndent()
        )

        val lsiWithNullName = buildLocalSecondaryIndex(null, ProjectionType.ALL, "column2")
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsiWithNullName))
            )
        }.isFailure().hasMessage(
            """
                Invalid table/index name.  Table/index names must be between 3 and 255 characters long, 
                and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'
            """.trimIndent()
        )
    }

    @Test
    fun `test no projection in index`() {
        val lsiNoProjection = buildLocalSecondaryIndex("indexName", null, "column3")
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsiNoProjection))
            )
        }.isFailure().hasMessage("Indexes must have a projection specified")
    }

    @Test
    fun `test no keySchema in index`() {
        val lsiNoKeySchema = buildLocalSecondaryIndex("indexName", ProjectionType.ALL)
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsiNoKeySchema))
            )
        }.isFailure().hasMessage("No defined key schema.  A key schema containing at least a hash key must be defined for all tables")
    }

    @Test
    fun `test wrong number of keySchemaElements`() {
        val lsi1 = buildLocalSecondaryIndex("indexName", ProjectionType.ALL, "column3")
        val lsi2 = buildLocalSecondaryIndex("indexName2", ProjectionType.ALL, "column3")
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsi1, lsi2))
            )
        }.isFailure().hasMessage(
            """
                The number of attributes in key schema must match the number of attributes defined in attribute definitions.
            """.trimIndent()
        )
    }

    @Test
    fun `test wrong keys order in index`() {
        val lsi = LocalSecondaryIndex.builder()
            .indexName("indexName")
            .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
            .keySchema(
                listOf(
                    KeySchemaElement.builder().attributeName("column3")
                        .keyType(KeyType.RANGE).build(),
                    KeySchemaElement.builder().attributeName("column1")
                        .keyType(KeyType.HASH).build()
                )
            )
            .build()
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsi))
            )
        }.isFailure().hasMessage(
            """
                Invalid key order. Hash Key must be specified first in key schema, Range Key must be specifed second
            """.trimIndent()
        )
    }

    @Test
    fun `test no range key in index`() {
        val lsi = LocalSecondaryIndex.builder()
            .indexName("indexName")
            .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
            .keySchema(
                listOf(
                    KeySchemaElement.builder().attributeName("column1")
                        .keyType(KeyType.HASH).build()
                )
            )
            .build()
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsi))
            )
        }.isFailure().hasMessage(
            """
                Local secondary indices must have a range key
            """.trimIndent()
        )
    }

    @Test
    fun `test no hash key in index`() {
        val lsi = LocalSecondaryIndex.builder()
            .indexName("indexName")
            .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
            .keySchema(
                listOf(
                    KeySchemaElement.builder().attributeName("column1")
                        .keyType(KeyType.RANGE).build()
                )
            )
            .build()
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsi))
            )
        }.isFailure().hasMessage(
            """
                No Hash Key specified in schema. All Dynamo DB tables must have exactly one hash key
            """.trimIndent()
        )
    }

    @Test
    fun `test too many hash keys in index`() {
        val lsi = LocalSecondaryIndex.builder()
            .indexName("indexName")
            .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
            .keySchema(
                listOf(
                    KeySchemaElement.builder().attributeName("column1")
                        .keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("column2")
                        .keyType(KeyType.HASH).build()
                )
            )
            .build()
        assertThat {
            mock.createTable(
                createTableWithIndex(listOf(lsi))
            )
        }.isFailure().hasMessage(
            """
                Too many hash keys specified. All Dynamo DB tables must have exactly one hash key
            """.trimIndent()
        )
    }

    @Test
    fun `test create index on hash table`() {
        val lsi = listOf(buildLocalSecondaryIndex("indexName", ProjectionType.ALL, "column3"))
        val numOfIndexes = lsi.size
        val attributeDefinitions = getAttributeDefinitions(2 + numOfIndexes)
        val hashKey = attributeToKey(attributeDefinitions[0], KeyType.HASH)

        assertThat {
            mock.createTable(
                CreateTableRequest.builder()
                    .tableName("testTable")
                    .attributeDefinitions(attributeDefinitions)
                    .keySchema(listOf(hashKey))
                    .localSecondaryIndexes(lsi)
                    .build()
            )
        }.isFailure().hasMessage(
            """
                Local Secondary indices are not allowed on hash tables, only hash and range tables
            """.trimIndent()
        )
    }

    @Test
    fun `test create table with index response`() {
        val firstIndexName = "indexName1"
        val secondIndexName = "indexName2"
        val partitionKeyName = "column1"
        val firstSortKeyName = "column3"
        val secondSortKeyName = "column4"
        val index1 = buildLocalSecondaryIndex(firstIndexName, ProjectionType.ALL, firstSortKeyName)
        val index2 = buildLocalSecondaryIndex(secondIndexName, ProjectionType.KEYS_ONLY, secondSortKeyName)

        val indexes = mock.createTable(createTableWithIndex(listOf(index1, index2))).tableDescription().localSecondaryIndexes()
        assertEquals(2, indexes.size)
        assertEquals(firstIndexName, indexes[0].indexName())
        assertEquals(secondIndexName, indexes[1].indexName())
        assertEquals(ProjectionType.ALL, indexes[0].projection().projectionType())
        assertEquals(ProjectionType.KEYS_ONLY, indexes[1].projection().projectionType())
        assertEquals(partitionKeyName, indexes[0].keySchema()[0].attributeName())
        assertEquals(partitionKeyName, indexes[1].keySchema()[0].attributeName())
        assertEquals(KeyType.HASH, indexes[0].keySchema()[0].keyType())
        assertEquals(KeyType.HASH, indexes[1].keySchema()[0].keyType())
        assertEquals(firstSortKeyName, indexes[0].keySchema()[1].attributeName())
        assertEquals(secondSortKeyName, indexes[1].keySchema()[1].attributeName())
        assertEquals(KeyType.RANGE, indexes[0].keySchema()[1].keyType())
        assertEquals(KeyType.RANGE, indexes[1].keySchema()[1].keyType())
    }
}
