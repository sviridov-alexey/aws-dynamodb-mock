package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.query.grammar.ProjectionExpressionGrammar
import ru.hse.dynamomock.model.sortKey
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
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

fun QueryRequest.retrieveAttributesTransformer(
    table: TableMetadata
): (Map<String, AttributeValue>) -> (Map<String, AttributeValue>) {
    val select = select() ?: if (indexName() == null) null else Select.ALL_PROJECTED_ATTRIBUTES

    val projectionExpression = projectionExpression()
    val attributesToGet = attributesToGet().takeIf { hasAttributesToGet() }

    if (select == Select.ALL_ATTRIBUTES || select == Select.COUNT) {
        dynamoRequires(projectionExpression == null && attributesToGet == null) {
            "ProjectionExpression and AttributesToGet must be null if Select != SPECIFIC_ATTRIBUTES"
        }
        return { it }
    }
    if (select == Select.ALL_PROJECTED_ATTRIBUTES) {
        dynamoRequires(indexName() != null) {
            "Select cannot be 'ALL_PROJECTED_ATTRIBUTES' without provided index."
        }
        dynamoRequires(projectionExpression == null && attributesToGet == null) {
            "ProjectionExpression and AttributesToGet must be null if select is 'ALL_PROJECTED_ATTRIBUTES'."
        }
        val index = table.localSecondaryIndex(indexName())
        val keys = listOfNotNull(table.partitionKey, table.sortKey, index.sortKey)
        return when (val projectionType = index.projection().projectionType()) {
            ProjectionType.ALL -> { attrs -> attrs }
            ProjectionType.KEYS_ONLY, ProjectionType.INCLUDE -> { attrs ->
                val result = attrs.filterKeys { it in keys }.apply {
                    dynamoRequires(size == 3 || size == 2 && table.sortKey == null) {
                        "Attributes must contain partition key, sort key (if presented) and index key."
                    }
                }.toMutableMap()

                if (projectionType === ProjectionType.INCLUDE) {
                    result += index.projection().nonKeyAttributes().mapNotNull { attr ->
                        attrs[attr]?.let { attr to it }
                    }
                }
                result
            }
            else -> throw dynamoException("Unknown type of projection in index ${index.indexName()}.")
        }
    }

    dynamoRequires(select == Select.SPECIFIC_ATTRIBUTES || select == null) {
        "Unknown Select in query."
    }

    dynamoRequires(attributesToGet == null || projectionExpression == null) {
        "Cannot specify AttributesToGet and ProjectionExpression at the same time."
    }

    // TODO take into account overlapped paths in projection
    when {
        projectionExpression != null -> {
            val grammar = ProjectionExpressionGrammar(expressionAttributeNames() ?: emptyMap())
            val projection = grammar.parse(projectionExpression)
            return { attrs ->
                projection.mapNotNull { attribute ->
                    val name = attribute.simpleName
                    if (name in attrs) {
                        attribute.retrieve(mapOf(name to attrs.getValue(name)))?.let { name to attrs.getValue(name) }
                    } else {
                        null
                    }
                }.distinctBy { it.first }.toMap()
            }
        }
        attributesToGet != null -> {
            if (attributesToGet != attributesToGet.distinct()) {
                throw dynamoException("AttributesToGet contain two identical attributes.")
            }
            return { attrs ->
                attributesToGet.mapNotNull { if (it in attrs) it to attrs.getValue(it) else null }.toMap()
            }
        }
        select == null -> {
            return { it }
        }
        else -> throw dynamoException(
            "Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES."
        )
    }
}
