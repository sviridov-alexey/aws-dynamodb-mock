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
    val partitionKey: Key,
    val sortKey: Key?,
    val items: List<AttributeInfo>
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
    val attributeName: String,
    val attributeType: String,
    val attributeValue: String
)

data class Key(
    val attributeName: String,
    val attributeType: String,
    val attributeValue: Any?
)