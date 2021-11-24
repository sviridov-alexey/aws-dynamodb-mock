package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.query.grammar.ProjectionExpressionGrammar
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.Select

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

fun QueryRequest.retrieveAttributesTransformer(): (Map<String, AttributeValue>) -> (Map<String, AttributeValue>) {
    require(indexName() == null || select() == Select.ALL_PROJECTED_ATTRIBUTES) {
        "Indexes are not supported in query yet."
    }

    if (select() == Select.ALL_ATTRIBUTES || select() == Select.COUNT) {
        return { it }
    }

    val projectionExpression = projectionExpression()
    val attributesToGet = attributesToGet().takeIf { hasAttributesToGet() }
    if (projectionExpression == null && attributesToGet == null) {
        return { it }
    }

    // TODO take into account overlapped paths in projection
    if (projectionExpression != null) {
        val grammar = ProjectionExpressionGrammar(expressionAttributeNames() ?: emptyMap())
        val projection = grammar.parse(projectionExpression)
        return { items ->
            projection.mapNotNull { attribute ->
                val name = attribute.simpleName
                if (name in items) {
                    attribute.retrieve(mapOf(name to items.getValue(name)))?.let { name to it }
                } else {
                    null
                }
            }.toMap()
        }
    } else if (attributesToGet != null) {
        return { items ->
            attributesToGet.mapNotNull { if (it in items) it to items.getValue(it) else null }.toMap()
        }
    } else {
        return { it }
    }
}
