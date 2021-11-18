package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.query.grammar.ProjectionExpressionGrammar
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.Select

sealed class QueryAttribute {
    abstract fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue?

    sealed class Simple : QueryAttribute() {
        data class Value(val name: String) : Simple() {
            override fun retrieve(attributeValues: Map<String, AttributeValue>) = attributeValues[name]
        }

        data class ListValue(val attribute: Simple, val index: Int) : Simple() {
            override fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue? =
                attribute.retrieve(attributeValues)?.l()?.getOrNull(index)
        }
    }

    data class MapValue(val map: Simple, val key: QueryAttribute) : QueryAttribute() {
        override fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue? =
            map.retrieve(attributeValues)?.let { key.retrieve(it.m()) }
    }
}

private fun QueryRequest.retrieveAttributesToGet(possibleAttributes: List<String>): List<QueryAttribute> {
    if (indexName() != null) {
        throw UnsupportedOperationException("Indexes are not supported yet.")
    }
    if (select() == Select.ALL_ATTRIBUTES || select() == Select.COUNT) {
        return possibleAttributes.map { QueryAttribute.Simple.Value(it) }
    } else if (select() == Select.ALL_PROJECTED_ATTRIBUTES) {
        throw UnsupportedOperationException("Indexes are not supported yet.")
    }

    val projectionExpression = projectionExpression()
    val attributesToGet = attributesToGet()
    require(projectionExpression == null || attributesToGet == null) {
        "At least one value from 'projectionExpression' and 'attributesToGet' must be null."
    }
    if (projectionExpression != null) {
        return ProjectionExpressionGrammar(expressionAttributeNames() ?: emptyMap()).parse(projectionExpression())
    }
    if (attributesToGet != null) {
        return attributesToGet.map { QueryAttribute.Simple.Value(it) }
    }
    return possibleAttributes.map { QueryAttribute.Simple.Value(it) }
}
