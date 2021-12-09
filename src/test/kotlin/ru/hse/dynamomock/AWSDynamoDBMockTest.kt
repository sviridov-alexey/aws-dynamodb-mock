package ru.hse.dynamomock

import org.junit.jupiter.api.BeforeEach
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant
import kotlin.properties.Delegates

internal open class AWSDynamoDBMockTest {
    protected var mock: AWSDynamoDBMock by Delegates.notNull()
        private set

    @BeforeEach
    fun init() {
        mock = AWSDynamoDBMock()
    }

    protected fun TableMetadata.toCreateTableRequest(): CreateTableRequest = CreateTableRequest.builder()
        .tableName(tableName)
        .attributeDefinitions(attributeDefinitions)
        .keySchema(
            listOfNotNull(
                KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build(),
                sortKey?.let { KeySchemaElement.builder().attributeName(it).keyType(KeyType.RANGE).build() }
            )
        )
        .localSecondaryIndexes(localSecondaryIndexes)
        .build()

    protected fun TableMetadata.toDeleteTableRequest(): DeleteTableRequest = DeleteTableRequest.builder()
        .tableName(tableName)
        .build()

    protected fun TableMetadata.toDescribeTableRequest(): DescribeTableRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build()

    protected fun putItemRequestBuilder(
        tableName: String,
        item: Map<String, AttributeValue>,
        returnValue: ReturnValue = ReturnValue.NONE
    ): PutItemRequest = PutItemRequest.builder()
        .tableName(tableName)
        .item(item)
        .returnValues(returnValue)
        .build()

    protected fun getItemRequestBuilder(
        tableName: String,
        attributes: List<String>,
        keys: Map<String, AttributeValue>
    ): GetItemRequest = GetItemRequest.builder()
        .tableName(tableName)
        .attributesToGet(attributes)
        .key(keys)
        .build()

    protected fun deleteItemRequestBuilder(
        tableName: String,
        keys: Map<String, AttributeValue>,
        returnValue: ReturnValue = ReturnValue.NONE
    ): DeleteItemRequest = DeleteItemRequest.builder()
        .tableName(tableName)
        .key(keys)
        .returnValues(returnValue)
        .build()

    companion object {
        @JvmStatic
        protected val attributeDefinitionPool = listOf(
            AttributeDefinition.builder().attributeName("myText").attributeType("S").build(),
            AttributeDefinition.builder().attributeName("firstAtt1").attributeType("S").build(),
            AttributeDefinition.builder().attributeName("12attr").attributeType("S").build(),
            AttributeDefinition.builder().attributeName("123_attr").attributeType("S").build(),
            AttributeDefinition.builder().attributeName("text2").attributeType("S").build(),
            AttributeDefinition.builder().attributeName("A111B").attributeType("N").build(),
            AttributeDefinition.builder().attributeName("A111B").attributeType("N").build(),
            AttributeDefinition.builder().attributeName("LOL_KEK_2").attributeType("N").build(),
            AttributeDefinition.builder().attributeName("what_is").attributeType("N").build(),
            AttributeDefinition.builder().attributeName("A111B").attributeType("N").build(),
            AttributeDefinition.builder().attributeName("Ke1k22").attributeType("B").build(),
            AttributeDefinition.builder().attributeName("mIsHa").attributeType("B").build(),
            AttributeDefinition.builder().attributeName("hehehe17").attributeType("B").build(),
            AttributeDefinition.builder().attributeName("saveMe").attributeType("B").build(),
            AttributeDefinition.builder().attributeName(List(100){ 'a' }.joinToString("")).attributeType("S").build()
        )

        private fun getKeySchemaElements(partitionKeyIndex: Int, lsiIndex: Int): List<KeySchemaElement> =
            listOf(
                KeySchemaElement.builder().attributeName(attributeDefinitionPool[partitionKeyIndex].attributeName())
                    .keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName(attributeDefinitionPool[lsiIndex].attributeName())
                    .keyType(KeyType.RANGE).build()
            )

        @JvmStatic
        protected fun localSecondaryIndexesPool(partitionKeyIndex: Int) = listOf(
            listOf(LocalSecondaryIndex.builder()
                .indexName("cooold")
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .keySchema(getKeySchemaElements(partitionKeyIndex, 1))
                .build()),
            listOf(LocalSecondaryIndex.builder()
                .indexName("sweaterweather")
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .keySchema(getKeySchemaElements(partitionKeyIndex, 1))
                .build(),
                LocalSecondaryIndex.builder()
                    .indexName("coldplay")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 4))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName("everyminute")
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 8))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("and")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 6))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("everyhour")
                    .projection(Projection.builder().projectionType(ProjectionType.INCLUDE).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 5))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("imisssu")
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 0))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName("1")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("2")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 6))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("3")
                    .projection(Projection.builder().projectionType(ProjectionType.INCLUDE).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 5))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("4")
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 0))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("5")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("6")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName("sameTableName")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build(),
                LocalSecondaryIndex.builder()
                    .indexName("sameTableName")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 6))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName("wrongIndexName,,,,,:)")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName(null)
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName("indexName")
                    .keySchema(getKeySchemaElements(partitionKeyIndex, 9))
                    .build()
            ),
            listOf(
                LocalSecondaryIndex.builder()
                    .indexName("indexName")
                    .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                    .build()
            )
        )


        @JvmStatic
        protected fun createTableMetadata(
            name: String,
            partitionKeyIndex: Int,
            sortKeyIndex: Int?,
            creationDateTime: Instant,
            lsiIndex: Int? = null,
        ) = TableMetadata(
            name,
            listOfNotNull(
                attributeDefinitionPool[partitionKeyIndex],
                sortKeyIndex?.let { attributeDefinitionPool[it] }
            ),
            attributeDefinitionPool[partitionKeyIndex].attributeName(),
            sortKeyIndex?.let { attributeDefinitionPool[it].attributeName() },
            TableStatus.ACTIVE,
            lsiIndex?.let{localSecondaryIndexesPool(partitionKeyIndex)[lsiIndex]},
            creationDateTime
        )

        @JvmStatic
        protected val metadataPool = listOf(
            createTableMetadata("metadata_First1", 8, 1, Instant.ofEpochMilli(2103102401234)),
            createTableMetadata("otHerMeta__data", 13, null, Instant.ofEpochMilli(11111)),
            createTableMetadata("kek_lol239_kek", 5, 5, Instant.now()),
            createTableMetadata("wow_wow_WoW", 9, null, Instant.now()),
            createTableMetadata("save_me._.pls", 2, null, Instant.ofEpochMilli(432534634)),
            createTableMetadata("ANOTHER.._AAAA", 1, 11, Instant.now()),
            createTableMetadata("ke.k1e_ke", 7, null, Instant.ofEpochMilli(991222222222222)),
            createTableMetadata("SantaClaus", 10, 8, Instant.ofEpochMilli(666)),
            createTableMetadata("El_lik_sir", 2, 7, Instant.ofEpochMilli(666)),
            createTableMetadata("beat", 4, 2, Instant.now(), 0),
            createTableMetadata("moon", 4, 2, Instant.now(), 1)
        ) + attributeDefinitionPool.indices.flatMap { i ->
            listOf(
                createTableMetadata("TEST_N_$i", i, null, Instant.ofEpochMilli(123241424222 * i)),
                createTableMetadata("TEST_M_$i", i, i, Instant.ofEpochMilli(9472938474 * i + 2))
            )
        }
    }
}
