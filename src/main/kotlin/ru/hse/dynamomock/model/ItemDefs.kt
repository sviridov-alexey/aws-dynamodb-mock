package ru.hse.dynamomock.model

import kotlinx.serialization.Serializable

@Suppress("unused")
data class DynamoItem(
    val partitionKey: String,
    val sortKey: String?,
    /*
    ...
    Dynamo Item definition goes here */
)

data class HSQLDBPutItemRequest(
    val tableName: String,
    val partitionKey: Key,
    val sortKey: Key?,
    val items: String
)

data class HSQLDBGetItemRequest(
    val tableName: String,
    val partitionKey: Key,
    val sortKey: Key?,
    val attributesToGet: List<String>
)

data class HSQLDBGetItemResponse(
    val items: List<AttributeInfo>
)


@Serializable
data class AttributeInfo(
    val attributeName: String,
    val attributeType: String,
    val attributeValue: String
)

data class Key(
    val attributeName: String,
    val attributeType: String,
    val attributeValue: Any?
)
