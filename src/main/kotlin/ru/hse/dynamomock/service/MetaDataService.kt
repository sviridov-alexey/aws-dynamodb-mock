package ru.hse.dynamomock.service

import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.AttributeInfo
import ru.hse.dynamomock.model.DynamoType
import ru.hse.dynamomock.model.Key
import ru.hse.dynamomock.model.NumKey
import ru.hse.dynamomock.model.StringKey
import ru.hse.dynamomock.model.toAttributeTypeInfo
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.util.Base64

fun toKey(keyName: String, attributeValue: AttributeValue): Pair<DynamoType, Key> =
    if (attributeValue.s() != null) {
        DynamoType.S to StringKey(keyName, attributeValue.s())
    } else if (attributeValue.n() != null) {
        DynamoType.N to NumKey(keyName, attributeValue.n().toBigDecimal())
    } else if (attributeValue.b() != null) {
        DynamoType.B to StringKey(keyName, Base64.getEncoder().encodeToString(attributeValue.b().asByteArray()))
    } else {
        throw dynamoException("Invalid attribute value type")
    }

fun getKeyFromMetadata(
    keyName: String,
    keys: Map<String, AttributeValue>,
    attributeDefinitions: List<AttributeDefinition>
): Key {
    val keyAttributeValue = keys[keyName] ?: throw dynamoException("One of the required keys was not given a value")
    val (keyType, key) = toKey(keyName, keyAttributeValue)
    val expectedKeyType = attributeDefinitions.firstOrNull { it.attributeName() == keyName }
        ?: throw dynamoException("One of the required keys was not given a value")
    dynamoRequires(expectedKeyType.attributeTypeAsString().uppercase() == keyType.name) {
        "One or more parameter values were invalid: Type mismatch for key"
    }
    return key
}

fun getSortKeyFromMetadata(
    sortKeyName: String?,
    keys: Map<String, AttributeValue>,
    attributeDefinitions: List<AttributeDefinition>
): Key? {
    sortKeyName ?: return null

    return getKeyFromMetadata(sortKeyName, keys, attributeDefinitions)
}

fun itemToAttributeInfo(item: Map<String, AttributeValue>) =
    item.map { (k, v) -> AttributeInfo(k, v.toAttributeTypeInfo()) }

fun checkAttributeValue(av: AttributeValue): DynamoType = when {
    av.n() != null -> DynamoType.N
    av.s() != null -> DynamoType.S
    av.b() != null -> DynamoType.B
    av.hasSs() -> DynamoType.SS
    av.hasNs() -> DynamoType.NS
    av.hasBs() -> DynamoType.BS
    av.hasL() -> DynamoType.L
    av.hasM() -> DynamoType.M
    else -> throw dynamoException("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
}