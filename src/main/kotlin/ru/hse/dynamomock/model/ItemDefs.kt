package ru.hse.dynamomock.model

import kotlinx.serialization.Serializable
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
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
    val s: String?,
    val n: String?,
    val b : String?,
    val ss: List<String>?,
    val ns: List<String>?,
    val bs: List<String>?,
    val m: Map<String, AttributeTypeInfo>?,
    val l: List<AttributeTypeInfo>?,
    val bool: Boolean?,
    val nul: Boolean?
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

    private fun requireExactlyOneValue() {
        if (notNullProperties.size != 1) {
            throw DynamoDbException.builder().message(
                "Supplied AttributeValue has more than one types set, must contain exactly one of the supported types"
            ).build()
        }
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
