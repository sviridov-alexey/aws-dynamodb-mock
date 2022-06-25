package ru.hse.dynamomock

import assertk.all
import assertk.assertThat
import assertk.assertions.hasClass
import assertk.assertions.isFailure
import assertk.assertions.messageContains
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.hse.dynamomock.model.compareDescription
import ru.hse.dynamomock.model.getPartitionKey
import ru.hse.dynamomock.model.getSortKey
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant

internal class AWSDynamoDBMockDDLTest : AWSDynamoDBMockTest() {
    private fun attributeToKey(attributeDefinition: AttributeDefinition, keyType: KeyType): KeySchemaElement {
        return KeySchemaElement.builder()
            .attributeName(attributeDefinition.attributeName())
            .keyType(keyType)
            .build()
    }

    private fun dropDynamoTable(tableName: String) {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
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
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10).writeCapacityUnits(5).build())
            .localSecondaryIndexes(indexes)
            .build()
    }

    @Test
    fun `test create table response`() {
        for (metadata in metadataPool) {
            val mockDescription = mock.createTable(metadata.toCreateTableRequest()).tableDescription()
            val dynamoDescription = client.createTable(metadata.toCreateTableRequest()).tableDescription()
            dropDynamoTable(metadata.tableName)
            assertEquals(metadata.tableName, mockDescription.tableName())
            assertEquals(metadata.tableName, dynamoDescription.tableName())

            assertEquals(metadata.attributeDefinitions, mockDescription.attributeDefinitions())
            assertEquals(metadata.attributeDefinitions, dynamoDescription.attributeDefinitions())

            assertEquals(
                listOfNotNull(metadata.partitionKey, metadata.sortKey),
                listOfNotNull(
                    getPartitionKey(mockDescription.keySchema()),
                    getSortKey(mockDescription.keySchema())
                )
            )

            assertEquals(
                listOfNotNull(metadata.partitionKey, metadata.sortKey),
                listOfNotNull(
                    getPartitionKey(dynamoDescription.keySchema()),
                    getSortKey(dynamoDescription.keySchema())
                )
            )

            assertEquals(metadata.tableStatus, TableStatus.ACTIVE)
            assertTrue(mockDescription.creationDateTime() <= Instant.now())
            assertTrue(dynamoDescription.creationDateTime() <= Instant.now().plusSeconds(5))
        }
    }

    @Test
    fun `test create table with same name`() {
        for ((i, metadata) in metadataPool.dropLast(1).withIndex()) {
            val request = metadata.toCreateTableRequest()
            mock.createTable(request)
            client.createTable(request)
            assertThrows<DynamoDbException> {
                mock.createTable(request)
            }
            assertThrows<DynamoDbException> {
                client.createTable(request)
            }
            val request2 = metadataPool[i + 1].toCreateTableRequest().toBuilder().tableName(request.tableName()).build()
            assertThrows<DynamoDbException> {
                mock.createTable(request2)
            }
            assertThrows<DynamoDbException> {
                client.createTable(request2)
            }
            dropDynamoTable(metadata.tableName)
        }
    }

    @Test
    fun `test drop table`() {
        for (metadata in metadataPool) {
            val createRequest = metadata.toCreateTableRequest()
            val dropRequest = metadata.toDeleteTableRequest()
            val describeDescription = metadata.toDescribeTableRequest()

            val mockCreateDescription = mock.createTable(createRequest).tableDescription()
            val mockDescription = mock.describeTable(describeDescription).table()
            val mockDeleteDescription = mock.deleteTable(dropRequest).tableDescription()

            val dynamoCreateDescription = client.createTable(createRequest).tableDescription()
            val dynamoDescription = client.describeTable(describeDescription).table()
            val dynamoDeleteDescription = client.deleteTable(dropRequest).tableDescription()
            assertEquals(mockCreateDescription, mockDescription)
            assertEquals(mockDescription, mockDeleteDescription)
            assertTrue(mockDescription.compareDescription(dynamoDescription))
            assertEquals(dynamoCreateDescription, dynamoDescription)
            assertEquals(dynamoDescription, dynamoDeleteDescription)
        }
    }

    @Test
    fun `test drop non-existent table`() {
        val request = metadataPool.last().toDeleteTableRequest()
        assertThrows<DynamoDbException> {
            mock.deleteTable(request)
        }
        assertThrows<DynamoDbException> {
            client.deleteTable(request)
        }
    }

    @Test
    fun `test drop table twice`() {
        for (metadata in metadataPool) {
            val createTableRequest = metadata.toCreateTableRequest()
            mock.createTable(createTableRequest)
            client.createTable(createTableRequest)
            val deleteRequest = metadata.toDeleteTableRequest()
            mock.deleteTable(deleteRequest)
            client.deleteTable(deleteRequest)
            assertThrows<DynamoDbException> {
                mock.deleteTable(deleteRequest)
            }
            assertThrows<DynamoDbException> {
                client.deleteTable(deleteRequest)
            }
        }
    }

    @Test
    fun `test describe table`() {
        for (metadata in metadataPool) {
            val createTableRequest = metadata.toCreateTableRequest()
            val describeTableRequest = metadata.toDescribeTableRequest()

            val mockDescription = mock.createTable(createTableRequest).tableDescription()
            val dynamoDescription = client.createTable(createTableRequest).tableDescription()
            assertEquals(mockDescription, mock.describeTable(describeTableRequest).table())
            assertEquals(dynamoDescription, client.describeTable(describeTableRequest).table())
            assertTrue(mockDescription.compareDescription(dynamoDescription))
            dropDynamoTable(metadata.tableName)
        }
    }

    @Test
    fun `test describe non-existent table`() {
        for (metadata in metadataPool.drop(1)) {
            val createTableRequest = metadata.toCreateTableRequest()
            mock.createTable(createTableRequest)
            client.createTable(createTableRequest)
            dropDynamoTable(metadata.tableName)
        }
        val describeTableRequest = metadataPool.first().toDescribeTableRequest()
        assertThrows<DynamoDbException> {
            mock.describeTable(describeTableRequest)
        }
        assertThrows<DynamoDbException> {
            client.describeTable(describeTableRequest)
        }
    }

    @Test
    fun `test more indexes than it should be`() {
        val manyLSI = buildLocalSecondaryIndex("manyIndex", ProjectionType.ALL, "column2")
        val errorMessage = "Cannot have more than 5 local secondary indexes per table"
        val createTableRequest = createTableWithIndex(listOf(manyLSI, manyLSI, manyLSI, manyLSI, manyLSI, manyLSI))

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            messageContains(errorMessage)
            hasClass(DynamoDbException::class)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test same indices name`() {
        val sameLSI = buildLocalSecondaryIndex("sameIndex", ProjectionType.ALL, "column2")
        val createTableRequest = createTableWithIndex(listOf(sameLSI, sameLSI))
        val errorMessage = "Two local secondary indicies have the same name"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test wrong index name`() {
        val lsiWithWrongName = buildLocalSecondaryIndex("wrongName,,,,", ProjectionType.ALL, "column2")
        val createTableRequest = createTableWithIndex(listOf(lsiWithWrongName))
        val errorMessage =
            "Invalid table/index name.  Table/index names must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        val lsiWithNullName = buildLocalSecondaryIndex(null, ProjectionType.ALL, "column2")
        val createTableRequest2 = createTableWithIndex(listOf(lsiWithNullName))
        assertThat {
            mock.createTable(createTableRequest2)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest2)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test no projection in index`() {
        val lsiNoProjection = buildLocalSecondaryIndex("indexName", null, "column3")
        val createTableRequest = createTableWithIndex(listOf(lsiNoProjection))
        val errorMessage = "Indexes must have a projection specified"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test no keySchema in index`() {
        val lsiNoKeySchema = buildLocalSecondaryIndex("indexName", ProjectionType.ALL)
        val createTableRequest = createTableWithIndex(listOf(lsiNoKeySchema))
        val errorMessage =
            "No defined key schema.  A key schema containing at least a hash key must be defined for all tables"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test wrong number of keySchemaElements`() {
        val lsi1 = buildLocalSecondaryIndex("indexName", ProjectionType.ALL, "column3")
        val lsi2 = buildLocalSecondaryIndex("indexName2", ProjectionType.ALL, "column3")
        val createTableRequest = createTableWithIndex(listOf(lsi1, lsi2))
        val errorMessage =
            "The number of attributes in key schema must match the number of attributesdefined in attribute definitions."

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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

        val createTableRequest = createTableWithIndex(listOf(lsi))
        val errorMessage =
            "Invalid key order.  Hash Key must be specified first in key schema, Range Key must be specifed second"
        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }

        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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
        val createTableRequest = createTableWithIndex(listOf(lsi))
        val errorMessage = "Local secondary indices must have a range key"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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
        val createTableRequest = createTableWithIndex(listOf(lsi))
        val errorMessage = "No Hash Key specified in schema.  All Dynamo DB tables must have exactly one hash key"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
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
        val createTableRequest = createTableWithIndex(listOf(lsi))
        val errorMessage = "Too many hash keys specified.  All Dynamo DB tables must have exactly one hash key"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test create index on hash table`() {
        val lsi = listOf(buildLocalSecondaryIndex("indexName", ProjectionType.ALL, "column3"))
        val numOfIndexes = lsi.size
        val attributeDefinitions = getAttributeDefinitions(2 + numOfIndexes)
        val hashKey = attributeToKey(attributeDefinitions[0], KeyType.HASH)

        val createTableRequest = CreateTableRequest.builder()
            .tableName("testTable")
            .attributeDefinitions(attributeDefinitions)
            .keySchema(listOf(hashKey))
            .localSecondaryIndexes(lsi)
            .build()
        val errorMessage = "Local Secondary indices are not allowed on hash tables, only hash and range tables"

        assertThat {
            mock.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
        assertThat {
            client.createTable(createTableRequest)
        }.isFailure().all {
            hasClass(DynamoDbException::class)
            messageContains(errorMessage)
        }
    }

    @Test
    fun `test create table with index response`() {
        val firstIndexName = "indexName1"
        val secondIndexName = "indexName2"
        val firstSortKeyName = "column3"
        val secondSortKeyName = "column4"
        val index1 = buildLocalSecondaryIndex(firstIndexName, ProjectionType.ALL, firstSortKeyName)
        val index2 = buildLocalSecondaryIndex(secondIndexName, ProjectionType.KEYS_ONLY, secondSortKeyName)

        val createTableRequest = createTableWithIndex(listOf(index1, index2))
        val mockIndexes = mock.createTable(createTableRequest).tableDescription().localSecondaryIndexes()
        val dynamoIndexes = client.createTable(createTableRequest).tableDescription().localSecondaryIndexes()
        dropDynamoTable("testTable")

        checkIndexes(mockIndexes, firstIndexName, secondIndexName, firstSortKeyName, secondSortKeyName)
        checkIndexes(dynamoIndexes, firstIndexName, secondIndexName, firstSortKeyName, secondSortKeyName)
    }

    private fun checkIndexes(
        indexes: List<LocalSecondaryIndexDescription>,
        firstIndexName: String,
        secondIndexName: String,
        firstSortKeyName: String,
        secondSortKeyName: String
    ) {
        val partitionKeyName = "column1"
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
