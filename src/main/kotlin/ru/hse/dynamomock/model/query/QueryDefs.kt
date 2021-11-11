package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.query.grammar.ProjectionExpressionGrammar
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.Select

sealed class QueryAttribute {
    data class Value(val name: String) : QueryAttribute()
    data class ListValue(val attribute: QueryAttribute, val index: Int) : QueryAttribute()
    data class MapValue(val attribute: QueryAttribute, val value: QueryAttribute) : QueryAttribute()
}

private fun QueryRequest.retrieveAttributesToGet(possibleAttributes: List<String>): List<QueryAttribute> {
    if (indexName() != null) {
        throw UnsupportedOperationException("Indexes are not supported yet.")
    }
    if (select() == Select.ALL_ATTRIBUTES || select() == Select.COUNT) {
        return possibleAttributes.map { QueryAttribute.Value(it) }
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
        return attributesToGet.map { QueryAttribute.Value(it) }
    }
    return possibleAttributes.map { QueryAttribute.Value(it) }
}
