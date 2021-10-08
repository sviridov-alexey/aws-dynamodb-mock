package ru.hse.dynamomock.db.util

object TypesConverter {
    fun fromDynamoToSqlType(type: String): String = when (type.lowercase()) {
        "s" -> "varchar(10000)" // TODO exclude constant
        "b" -> "bit"
        "n" -> "bigint" // TODO it should be able to store any number
        "bytebuffer" -> TODO()
        "ss" -> TODO()
        "ns" -> TODO()
        "bs" -> TODO()
        else -> type
    }
}