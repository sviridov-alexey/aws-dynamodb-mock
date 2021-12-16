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
        .localSecondaryIndexes(localSecondaryIndexes.values)
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

        private fun buildLocalSecondaryIndex(
            indexName: String?, projectionType: ProjectionType,
            partitionKeyIndex: Int, lsiIndex: Int) =
            LocalSecondaryIndex.builder()
            .indexName(indexName)
            .projection(Projection.builder().projectionType(projectionType).build())
            .keySchema(getKeySchemaElements(partitionKeyIndex, lsiIndex))
            .build()

        @JvmStatic
        protected fun localSecondaryIndexesPool(partitionKeyIndex: Int) = listOf(
            listOf(buildLocalSecondaryIndex("cooold", ProjectionType.ALL, partitionKeyIndex, 1)),
            listOf(
                buildLocalSecondaryIndex("sweaterweather", ProjectionType.ALL, partitionKeyIndex, 1),
                buildLocalSecondaryIndex("coldplay", ProjectionType.KEYS_ONLY, partitionKeyIndex, 4),
            ),
            listOf(
                buildLocalSecondaryIndex("everyminute", ProjectionType.ALL, partitionKeyIndex, 8),
                buildLocalSecondaryIndex("and", ProjectionType.KEYS_ONLY, partitionKeyIndex, 6),
                buildLocalSecondaryIndex("everyhour", ProjectionType.INCLUDE, partitionKeyIndex, 5),
                buildLocalSecondaryIndex("imisssu", ProjectionType.ALL, partitionKeyIndex, 0)
            ),
            listOf(
                buildLocalSecondaryIndex("1", ProjectionType.KEYS_ONLY, partitionKeyIndex, 9),
                buildLocalSecondaryIndex("2", ProjectionType.KEYS_ONLY, partitionKeyIndex, 6),
                buildLocalSecondaryIndex("3", ProjectionType.INCLUDE, partitionKeyIndex, 5),
                buildLocalSecondaryIndex("4", ProjectionType.ALL, partitionKeyIndex, 0),
                buildLocalSecondaryIndex("5", ProjectionType.KEYS_ONLY, partitionKeyIndex, 9),
                buildLocalSecondaryIndex("6", ProjectionType.KEYS_ONLY, partitionKeyIndex, 9)
            ),
            listOf(
                buildLocalSecondaryIndex("sameTableName", ProjectionType.KEYS_ONLY, partitionKeyIndex, 9),
                buildLocalSecondaryIndex("sameTableName", ProjectionType.KEYS_ONLY, partitionKeyIndex, 6)
            ),
            listOf(
                buildLocalSecondaryIndex("wrongIndexName,,,,,:)", ProjectionType.KEYS_ONLY, partitionKeyIndex, 9)
            ),
            listOf(
                buildLocalSecondaryIndex(null, ProjectionType.KEYS_ONLY, partitionKeyIndex, 9)
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
            lsiIndex?.let{
                val list = localSecondaryIndexesPool(partitionKeyIndex)[lsiIndex]
                list.associateBy { it.indexName()!! }
            } ?: emptyMap(),
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
