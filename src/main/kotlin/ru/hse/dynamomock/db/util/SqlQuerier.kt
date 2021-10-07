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
}
