package ru.hse.dynamomock.db.util

import ru.hse.dynamomock.model.TableMetadata

object SqlQuerier {
    fun createTableQuery(tableMetadata: TableMetadata): String {
        require(tableMetadata.attributeDefinitions.isNotEmpty())
        val columns = tableMetadata.attributeDefinitions.joinToString(separator = ",") {
            "${it.attributeName()} ${TypesConverter.fromDynamoToSqlType(it.attributeType().name)}"
        }
        val primaryKey = tableMetadata.keySchema.joinToString(separator = ",") {
            it.attributeName()
        }

        //language=SQL
        return """
            CREATE TABLE ${tableMetadata.tableName} (
                $columns
                ${if (primaryKey.isNotEmpty()) ", PRIMARY KEY ($primaryKey)" else ""}
            )
        """
    }

    fun putItemQuery(tableName: String, items: List<AttributeInfo>): String {
        val columnNames = mutableListOf<String>()
        val values = mutableListOf<Any?>()
        items.forEach {
            columnNames.add(it.attributeName)
            values.add(it.attribute)
        }

        //language=SQL
        return """
            INSERT INTO $tableName (${columnNames.joinToString(", ")})
            VALUES (${values.joinToString(", ") {when (it) {
                is String -> "'$it'"
                is Boolean -> "$it"
                // TODO: is Number, but in dynamo number is string too (float?? int???)
                else -> "$it"
        }}});
        """
    }

    fun getItemQuery(tableName: String, partitionKey: AttributeInfo, attributesToGet: List<String>): String {
        //language=SQL
        return """
            SELECT ${attributesToGet.joinToString(", ")} FROM $tableName
            WHERE ${partitionKey.attributeName}=${when (partitionKey.attribute) {
            is String -> "'${partitionKey.attribute}'"
            is Boolean -> "${partitionKey.attribute}"
            // TODO: other types
            else -> "${partitionKey.attribute}"
        }};
        """
    }
}
