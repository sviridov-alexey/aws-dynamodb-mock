package ru.hse.dynamomock.model.query

sealed class QueryAttribute {
    data class Value(val name: String) : QueryAttribute()
    data class ListValue(val attribute: QueryAttribute, val index: Int) : QueryAttribute()
    data class MapValue(val attribute: QueryAttribute, val value: QueryAttribute) : QueryAttribute()
}
