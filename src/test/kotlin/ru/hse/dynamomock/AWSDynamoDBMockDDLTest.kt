package ru.hse.dynamomock

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Instant
import kotlin.random.Random

internal class AWSDynamoDBMockDDLTest : AWSDynamoDBMockTest() {
    private fun TableMetadata.toCreateTableRequest() = CreateTableRequest.builder()
        .tableName(tableName)
        .attributeDefinitions(attributeDefinitions)
        .keySchema(
            listOfNotNull(
                KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build(),
                sortKey?.let { KeySchemaElement.builder().attributeName(it).keyType(KeyType.RANGE).build() }
            )
        ).build()

    private fun TableMetadata.toDeleteTableRequest() = DeleteTableRequest.builder()
        .tableName(tableName)
        .build()

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
        `test create table response`(RandomTableMetadataGenerator.generate())
    }

    @Test
    fun `test create table with same name`() {
        val request = RandomTableMetadataGenerator.generate().toCreateTableRequest()
        mock.createTable(request)
        assertThrows<IllegalArgumentException> {
            mock.createTable(request)
        }
        val request2 = RandomTableMetadataGenerator.generate().toCreateTableRequest()
            .toBuilder().tableName(request.tableName()).build()
        assertThrows<IllegalArgumentException> {
            mock.createTable(request2)
        }
    }

    @Test
    fun `test drop table`() {
        val metadata = RandomTableMetadataGenerator.generate()
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
        val request = RandomTableMetadataGenerator.generate().toDeleteTableRequest()
        assertThrows<IllegalArgumentException> {
            mock.deleteTable(request)
        }
    }

    @Test
    fun `test drop table twice`() {
        val metadata = RandomTableMetadataGenerator.generate()
        val createRequest = metadata.toCreateTableRequest()
        val deleteRequest = metadata.toDeleteTableRequest()
        mock.createTable(createRequest)
        mock.deleteTable(deleteRequest)
        assertThrows<IllegalArgumentException> {
            mock.deleteTable(deleteRequest)
        }
    }
}

private object RandomTableMetadataGenerator {
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
        return@lazy listOf("S", "N", "B")::random
    }

    private fun generateAttributeDefinition() = AttributeDefinition.builder()
        .attributeName(nameGenerator(1..256))
        .attributeType(typeGenerator())
        .build()
}
