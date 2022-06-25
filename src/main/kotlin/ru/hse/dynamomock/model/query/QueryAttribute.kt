package ru.hse.dynamomock.model.query

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

sealed interface QueryAttribute {
    fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue?

    val simpleName: String
        get() = when (this) {
            is Simple.Value -> name
            is Simple.ListValue -> attribute.simpleName
            is MapValue -> map.simpleName
        }

    sealed interface Simple : QueryAttribute {
        data class Value(val name: String) : Simple {
            override fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue? =
                attributeValues[name]
        }

        data class ListValue(val attribute: Simple, val index: Int) : Simple {
            override fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue? =
                attribute.retrieve(attributeValues)?.l()?.getOrNull(index)
        }
    }

    data class MapValue(val map: Simple, val key: QueryAttribute) : QueryAttribute {
        override fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue? =
            map.retrieve(attributeValues)?.let { key.retrieve(it.m()) }
    }
}
