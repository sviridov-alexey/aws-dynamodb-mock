package ru.hse.dynamomock.model

import ru.hse.dynamomock.db.util.AttributeInfo

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
    val itemsList: List<AttributeInfo>
)

data class HSQLDBGetItemRequest(
    val tableName: String,
    val partitionKey: AttributeInfo,
    val attributesToGet: List<String>
)
