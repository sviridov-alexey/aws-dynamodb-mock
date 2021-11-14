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
