package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.query.grammar.ProjectionExpressionGrammar
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
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
    if (select() != Select.SPECIFIC_ATTRIBUTES && select() != null) {
        throw DynamoDbException.builder().message("Unknown Select in query.").build()
    }

    val projectionExpression = projectionExpression()
    val attributesToGet = attributesToGet().takeIf { hasAttributesToGet() }

    // TODO take into account overlapped paths in projection
    when {
        projectionExpression != null -> {
            val grammar = ProjectionExpressionGrammar(expressionAttributeNames() ?: emptyMap())
            val projection = grammar.parse(projectionExpression)
            return { items ->
                projection.mapNotNull { attribute ->
                    val name = attribute.simpleName
                    if (name in items) {
                        attribute.retrieve(mapOf(name to items.getValue(name)))?.let { name to items.getValue(name) }
                    } else {
                        null
                    }
                }.distinctBy { it.first }.toMap()
            }
        }
        attributesToGet != null -> return { items ->
            attributesToGet.mapNotNull { if (it in items) it to items.getValue(it) else null }.toMap()
        }
        select() == null -> return { it }
        else -> throw DynamoDbException.builder()
            .message("Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES.")
            .build()
    }
}
