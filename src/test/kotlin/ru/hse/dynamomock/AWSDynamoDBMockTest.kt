package ru.hse.dynamomock

import org.junit.jupiter.api.BeforeEach
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant
import kotlin.properties.Delegates
import kotlin.random.Random

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
        ).build()

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

    // TODO delete after tests refactoring
    protected object RandomDataGenerator {
        fun generate(): TableMetadata {
            val definitions = (1..Random.nextInt(1, 20)).map { generateAttributeDefinition() }
            return TableMetadata(
                nameGenerator(3..256),
                definitions,
                definitions.random().attributeName(),
                if (Random.nextInt() % 4 != 0) definitions.random().attributeName() else null,
                TableStatus.ACTIVE
            )
        }

        private val nameGenerator: (IntRange) -> String by lazy {
            val chars = ('a'..'z') + ('A'..'Z')
            val allowed = chars + ('0'..'9') + '-' + '_' // TODO support '.'
            return@lazy {
                chars.random() + (1..(it.first until it.last).random()).map { allowed.random() }.joinToString("")
            }
        }

        private val typeGenerator: () -> String by lazy {
            return@lazy listOf("S", "B", "N")::random
        }

        private fun generateAttributeDefinition() = AttributeDefinition.builder()
            .attributeName(nameGenerator(1..256))
            .attributeType(typeGenerator())
            .build()

        private fun generateAttributeValue(attributeDefinition: AttributeDefinition) = when(attributeDefinition.attributeTypeAsString()) {
            // TODO: BIGDECIMAL??? "N" -> AttributeValue.builder().n(BigDecimal(Math.random()).toString()).build()
            "N" -> AttributeValue.builder().n(Random.nextInt().toString()).build()
            "B" -> {
                val arr = ByteArray(20)
                AttributeValue.builder().b(SdkBytes.fromByteArray(Random.nextBytes(arr))).build()
            }
            "S" -> AttributeValue.builder().s(nameGenerator(1..128)).build()
            else -> AttributeValue.builder().s(nameGenerator(1..128)).build()
            // TODO: test other types
        }

        fun generateItem(attributeDefinitions: List<AttributeDefinition>) : Map<String, AttributeValue>{
            return attributeDefinitions.associate { it.attributeName() to generateAttributeValue(it) }
        }

    }

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

        @JvmStatic
        protected fun createTableMetadata(
            name: String,
            attributesIndices: Iterable<Int>,
            partitionKeyIndex: Int,
            sortKeyIndex: Int?,
            creationDateTime: Instant
        ) = TableMetadata(
            name,
            attributeDefinitionPool.withIndex().filter { (i, _) -> i in attributesIndices }.map { (_, x) -> x },
            attributeDefinitionPool[partitionKeyIndex].attributeName(),
            sortKeyIndex?.let { attributeDefinitionPool[it].attributeName() },
            TableStatus.ACTIVE,
            creationDateTime
        )

        @JvmStatic
        protected val metadataPool = listOf(
            createTableMetadata("metadata_First1", listOf(1, 4, 8, 9), 8, 1, Instant.ofEpochMilli(2103102401234)),
            createTableMetadata("otHerMeta__data", 1..14, 13, null, Instant.ofEpochMilli(11111)),
            createTableMetadata("kek_lol239_kek", 2..5, 5, 5, Instant.now()),
            createTableMetadata("wow_wow_WoW", listOf(13, 12, 9, 14), 9, null, Instant.now()),
            createTableMetadata("save_me._.pls", listOf(9, 1, 8, 2), 2, null, Instant.ofEpochMilli(432534634)),
            createTableMetadata("ANOTHER.._AAAA", listOf(1, 14), 1, 1, Instant.now()),
            createTableMetadata("ke.k1e_ke", 7..10, 7, null, Instant.ofEpochMilli(991222222222222)),
            createTableMetadata("SantaClaus", listOf(1, 2, 7, 3, 10, 8), 10, 8, Instant.ofEpochMilli(666)),
        ) + attributeDefinitionPool.indices.flatMap { i ->
            listOf(
                createTableMetadata("TEST_N_$i", listOf(i), i, null, Instant.ofEpochMilli(123241424222 * i)),
                createTableMetadata("TEST_M_$i", listOf(i), i, i, Instant.ofEpochMilli(9472938474 * i + 2))
            )
        }
    }
}
