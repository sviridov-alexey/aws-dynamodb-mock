package ru.hse.dynamomock.model

import com.google.gson.Gson

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

data class AttributeInfo(
    val attributeName: String,
    val attributeType: String,
    val attributeValue: Any?
) {
    companion object {
        private val gson = Gson() // TODO make accessible for all structures (probably will be required)

        fun AttributeInfo.toJson(): String = gson.toJson(this)

        fun fromJson(json: String): AttributeInfo = gson.fromJson(json, AttributeInfo::class.java)
    }
}
