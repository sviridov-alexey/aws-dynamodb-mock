package ru.hse.dynamomock

import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.net.URI
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

fun Boolean.asDynamoBoolValue(): AttributeValue = AttributeValue.builder()
    .bool(this)
    .build()

fun String.asDynamoStringValue(): AttributeValue = AttributeValue.builder()
    .s(this)
    .build()

fun String.asDynamoNumValue(): AttributeValue = AttributeValue.builder()
    .n(this)
    .build()

fun SdkBytes.asDynamoValue(): AttributeValue = AttributeValue.builder()
    .b(this)
    .build()

fun Boolean.asDynamoNulValue(): AttributeValue = AttributeValue.builder()
    .nul(this)
    .build()

fun List<String>.asDynamoStrListValue(): AttributeValue = AttributeValue.builder()
    .ss(this)
    .build()

fun List<String>.asDynamoNumListValue(): AttributeValue = AttributeValue.builder()
    .ns(this)
    .build()

fun List<SdkBytes>.asDynamoValue(): AttributeValue = AttributeValue.builder()
    .bs(this)
    .build()

fun Collection<AttributeValue>.asDynamoValue(): AttributeValue = AttributeValue.builder()
    .l(this)
    .build()

fun Map<String, AttributeValue>.asDynamoValue(): AttributeValue = AttributeValue.builder()
    .m(this)
    .build()

fun compareItems(item1: Map<String, AttributeValue>, item2: Map<String, AttributeValue>) {
    assertEquals(item1.size, item2.size)
    item1.entries.forEach {
        val v1 = it.value
        val v2 = item2[it.key]
        assertNotNull(v2)
        assertEquals(v1.s(), v2.s())
        assertEquals(v1.n(), v2.n())
        assertEquals(v1.b(), v2.b())
        assertEquals(v1.ss().toMutableList().sorted(), v2.ss().toMutableList().sorted())
        assertEquals(v1.ns().toMutableList().sorted(), v2.ns().toMutableList().sorted())
        assertEquals(v1.bs(), v2.bs())
        assertEquals(v1.l(), v2.l())
        assertEquals(v1.m(), v2.m())
        assertEquals(v1.nul(), v2.nul())
        assertEquals(v1.bool(), v2.bool())
    }
}

@Testcontainers
internal open class AWSDynamoDBMockTest {
    protected lateinit var mock: AWSDynamoDBMock
        private set
    protected lateinit var client: DynamoDbClient

    @BeforeEach
    fun init() {
        if (::mock.isInitialized) {
            mock.close()
        }
        mock = AWSDynamoDBMock()
    }

    @BeforeEach
    fun initDynamo() {
        val endpointUrl = java.lang.String.format("http://localhost:%d", dynamoDb.firstMappedPort)
        client = DynamoDbClient.builder()
            .endpointOverride(URI.create(endpointUrl)) // The region is meaningless for local DynamoDb but required for client builder validation
            .region(Region.EU_CENTRAL_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("dummy-key", "dummy-secret")
                )
            )
            .build()
    }

    protected fun TableMetadata.toCreateTableRequest(): CreateTableRequest = CreateTableRequest.builder()
        .tableName(tableName)
        .attributeDefinitions(attributeDefinitions)
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10).writeCapacityUnits(5).build())
        .keySchema(
            listOfNotNull(
                KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build(),
                sortKey?.let { KeySchemaElement.builder().attributeName(it).keyType(KeyType.RANGE).build() }
            )
        )
        .localSecondaryIndexes(if (localSecondaryIndexes.isEmpty()) null else localSecondaryIndexes.values)
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

    protected fun updateItemRequestBuilder(
        tableName: String,
        keys: Map<String, AttributeValue>,
        attributeUpdates: Map<String, AttributeValueUpdate>,
        returnValue: ReturnValue = ReturnValue.NONE
    ): UpdateItemRequest = UpdateItemRequest.builder()
        .tableName(tableName)
        .key(keys)
        .attributeUpdates(attributeUpdates)
        .returnValues(returnValue)
        .build()

    protected fun attributeValueUpdateBuilder(
        action: AttributeAction,
        value: AttributeValue? = null
    ): AttributeValueUpdate = AttributeValueUpdate.builder()
        .action(action)
        .value(value)
        .build()

    protected fun keysFromItem(
        item: Map<String, AttributeValue>,
        partKeyName: String,
        sortKeyName: String? = null
    ) =
        item.entries.filter { i -> i.key == partKeyName || (sortKeyName != null && i.key == sortKeyName) }
            .associate { it.key to it.value }

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
            AttributeDefinition.builder().attributeName(List(100) { 'a' }.joinToString("")).attributeType("S").build()
        )

        @JvmStatic
        protected fun createTableMetadata(
            name: String,
            partitionKeyIndex: Int,
            sortKeyIndex: Int?,
            creationDateTime: Instant
        ) = TableMetadata(
            name,
            listOfNotNull(
                attributeDefinitionPool[partitionKeyIndex],
                sortKeyIndex?.let { attributeDefinitionPool[it] }
            ),
            attributeDefinitionPool[partitionKeyIndex].attributeName(),
            sortKeyIndex?.let { attributeDefinitionPool[it].attributeName() },
            TableStatus.ACTIVE,
            emptyMap(),
            creationDateTime
        )

        @JvmStatic
        protected val metadataPool = listOf(
            createTableMetadata("metadata_First1", 8, 1, Instant.ofEpochMilli(2103102401234)),
            createTableMetadata("otHerMeta__data", 13, null, Instant.ofEpochMilli(11111)),
            createTableMetadata("kek_lol239_kek", 5, 7, Instant.now()),
            createTableMetadata("wow_wow_WoW", 9, null, Instant.now()),
            createTableMetadata("save_me._.pls", 2, null, Instant.ofEpochMilli(432534634)),
            createTableMetadata("ANOTHER.._AAAA", 1, 11, Instant.now()),
            createTableMetadata("ke.k1e_ke", 7, null, Instant.ofEpochMilli(991222222222222)),
            createTableMetadata("SantaClaus", 10, 8, Instant.ofEpochMilli(666)),
            createTableMetadata("El_lik_sir", 2, 7, Instant.ofEpochMilli(666))
        ) + attributeDefinitionPool.indices.flatMap { i ->
            listOf(
                createTableMetadata("TEST_N_$i", i, null, Instant.ofEpochMilli(123241424222 * i)),
            )
        }

        @Container
        val dynamoDb = GenericContainer("amazon/dynamodb-local:1.13.2")
            .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb")
            .withExposedPorts(8000)
    }
}
