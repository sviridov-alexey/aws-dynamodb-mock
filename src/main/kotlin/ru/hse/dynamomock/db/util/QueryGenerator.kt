package ru.hse.dynamomock.db.util

import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

class QueryGenerator {
    fun putItem(request: PutItemRequest): String {
        var columnNames = mutableListOf<String>()
        val values = request.item().forEach {
            columnNames.add(it.key)

        }
        return """
            INSERT INTO ${request.tableName()} (${columnNames})
            VALUES (${values});
        """
    }

}