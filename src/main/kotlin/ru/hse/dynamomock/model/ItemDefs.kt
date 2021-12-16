package ru.hse.dynamomock.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import ru.hse.dynamomock.exception.dynamoRequires
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal
import java.util.*

@Suppress("unused")
data class DynamoItem(
    val partitionKey: String,
    val sortKey: String?,
    /*
    ...
    Dynamo Item definition goes here */
)

data class DBPutItemRequest(
    val tableName: String,
    val partitionKey: Key,
    val sortKey: Key?,
    val fieldValues: List<AttributeInfo>
)

data class DBGetItemRequest(
    val tableName: String,
    val partitionKey: Key,
    val sortKey: Key?,
)

data class DBDeleteItemRequest(
    val tableName: String,
    val partitionKey: Key,
    val sortKey: Key?,
)

@Serializable
data class AttributeInfo(
    val name: String,
    val type: AttributeTypeInfo
)

@Serializable
data class AttributeTypeInfo(
    val s: String? = null,
    val n: String? = null,
    val b : String? = null,
    val ss: List<String>? = null,
    val ns: List<String>? = null,
    val bs: List<String>? = null,
    val m: Map<String, AttributeTypeInfo>? = null,
    val l: List<AttributeTypeInfo>? = null,
    val bool: Boolean? = null,
    val nul: Boolean? = null
) {
    @kotlinx.serialization.Transient
    private val notNullProperties = listOfNotNull(
        s?.let { "S" to it },
        n?.let { "N" to it },
        b?.let { "B" to it },
        ss?.let { "SS" to it },
        ns?.let { "NS" to it },
        bs?.let { "BS" to it },
        m?.let { "M" to it },
        l?.let { "L" to it },
        bool?.let { "BOOL" to it },
        nul?.let { "NULL" to it }
    )

    fun toAttributeValue(): AttributeValue = AttributeValue.builder()
        .s(s)
        .n(n)
        .b(b?.let { SdkBytes.fromByteArray(Base64.getDecoder().decode(b)) })
        .ss(ss)
        .ns(ns)
        .bs(bs?.map { SdkBytes.fromByteArray(Base64.getDecoder().decode(it)) })
        .m(m?.mapValues { it.value.toAttributeValue() })
        .l(l?.map { it.toAttributeValue() })
        .bool(bool)
        .nul(nul)
        .build()

    private fun requireExactlyOneValue() = dynamoRequires(notNullProperties.size == 1) {
        "Supplied AttributeValue has more than one types set, must contain exactly one of the supported types"
    }

    val typeAsString
        get(): String {
            requireExactlyOneValue()
            return notNullProperties.single().first
        }

    @Suppress("UNCHECKED_CAST")
    val value
        get(): Any {
            requireExactlyOneValue()
            val rawValue = notNullProperties.single().second
            return when (notNullProperties.single().first) {
                "N" -> (rawValue as String).toBigDecimal()
                "NS" -> (rawValue as List<String>).map { it.toBigDecimal() }.toSet()
                "BS" -> (rawValue as List<*>).toSet()
                "SS" -> (rawValue as List<*>).toSet()
                else -> rawValue
            }
        }
}

sealed class Key(
    val attributeName: String
)

class StringKey(
    attributeName: String,
    val attributeValue: String
) : Key(attributeName)

class NumKey(
    attributeName: String,
    val attributeValue: BigDecimal
) : Key(attributeName)

enum class DynamoType {
    S,
    N,
    B,
    SS,
    NS,
    BS,
    BOOL,
    NULL,
    M,
    L
}

enum class ImportableType {
    S,
    N,
    SS,
    NS,
    BOOL,
    NULL,
    M,
    L
}

fun toAttributeValue(type: String, element: String): AttributeValue = when (ImportableType.valueOf(type)) {
    ImportableType.S -> AttributeValue.builder().s(element).build()
    ImportableType.N -> AttributeValue.builder().n(element).build()
    ImportableType.NS -> {
        val list = element.split(",")
        AttributeValue.builder().ns(list).build()
    }
    ImportableType.SS -> {
        val list = element.split(",")
        AttributeValue.builder().ss(list).build()
    }
    ImportableType.NULL -> {
        AttributeValue.builder().nul(element == "true").build()
    }
    ImportableType.BOOL -> {
        AttributeValue.builder().bool(element == "true").build()
    }
    ImportableType.L -> {
        val attributeValues = Json.decodeFromString<List<AttributeTypeInfo>>(element)
        AttributeValue.builder()
            .l(attributeValues.map { v -> v.toAttributeValue() })
            .build()
    }
    ImportableType.M -> {
        val attributeValues = Json.decodeFromString<Map<String, AttributeTypeInfo>>(element)
        AttributeValue.builder().m(
            attributeValues.map { v -> v.key to v.value.toAttributeValue() }.toMap()
        ).build()
    }
}

fun AttributeValue.toAttributeTypeInfo(): AttributeTypeInfo = AttributeTypeInfo(
    s = s(),
    n = n(),
    b = b()?.let { Base64.getEncoder().encodeToString(b().asByteArray()) },
    ss = ss().takeIf { hasSs() },
    ns = ns().takeIf { hasNs() },
    bs = bs().map {  Base64.getEncoder().encodeToString(it.asByteArray()) }.takeIf { hasBs() },
    m = m().mapValues { it.value.toAttributeTypeInfo() }.takeIf { hasM() },
    l = l()?.map { it.toAttributeTypeInfo() }.takeIf { hasL() },
    bool = bool(),
    nul = nul()
)

