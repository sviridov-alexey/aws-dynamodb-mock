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

data class DBPutItemRequest(
    val tableName: String,
    val partitionKey: Any,
    val sortKey: Any?,
    val items: String
)

data class DBGetItemRequest(
    val tableName: String,
    val partitionKey: Any,
    val sortKey: Any?,
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
