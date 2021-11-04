package ru.hse.dynamomock

import org.junit.jupiter.api.BeforeEach
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue
import software.amazon.awssdk.services.dynamodb.model.TableStatus
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

    protected fun putItemRequestBuilder(tableName: String, item: Map<String, AttributeValue>, returnValue: ReturnValue = ReturnValue.NONE): PutItemRequest =
        PutItemRequest.builder()
            .tableName(tableName)
            .item(item)
            .returnValues(returnValue)
            .build()

    protected fun getItemRequestBuilder(tableName: String, attributes: List<String>, keys: Map<String, AttributeValue>): GetItemRequest =
        GetItemRequest.builder()
            .tableName(tableName)
            .attributesToGet(attributes)
            .key(keys)
            .build()

    protected fun deleteItemRequestBuilder(tableName: String, keys: Map<String, AttributeValue>, returnValue: ReturnValue = ReturnValue.NONE): DeleteItemRequest =
        DeleteItemRequest.builder()
            .tableName(tableName)
            .key(keys)
            .returnValues(returnValue)
            .build()

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

}


