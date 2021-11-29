package ru.hse.dynamomock.model

import kotlinx.serialization.Serializable
import java.math.BigDecimal

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

)

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
