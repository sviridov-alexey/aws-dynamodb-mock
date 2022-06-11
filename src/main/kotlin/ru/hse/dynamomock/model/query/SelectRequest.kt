package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.query.grammar.ConditionExpressionGrammar
import ru.hse.dynamomock.model.query.grammar.ProjectionExpressionGrammar
import ru.hse.dynamomock.model.sortKey
import software.amazon.awssdk.services.dynamodb.model.*

data class SelectRequest(
    val tableName: String,
    val indexName: String?,
    val attributesToGet: List<String>?,
    val limit: Int?,
    val select: Select?,
    val keyConditions: Map<String, Condition>?,
    val filter: Map<String, Condition>?,
    val conditionalOperator: ConditionalOperator?,
    val scanIndexForward: Boolean,
    val exclusiveStartKey: Map<String, AttributeValue>?,
    val returnConsumedCapacity: ReturnConsumedCapacity?,
    val projectionExpression: String?,
    val filterExpression: String?,
    val keyConditionExpression: String?,
    val expressionAttributeNames: Map<String, String>,
    val expressionAttributeValues: Map<String, AttributeValue>,
    val consistentRead: Boolean?,
    val hasKeyConditions: Boolean
) {
    fun retrieveFilterExpression(): Pair<ConditionExpression?, Set<String>> {
        return if (filter != null) {
            retrieveConditionExpression(conditionalOperator ?: ConditionalOperator.AND) to emptySet()
        } else if (filterExpression != null) {
            val grammar = ConditionExpressionGrammar(expressionAttributeNames, expressionAttributeValues)
            grammar.parse(filterExpression) to grammar.getUsedExpressionAttributes()
        } else {
            null to emptySet()
        }
    }

    private fun retrieveConditionExpression(conditionalOperator: ConditionalOperator): ConditionExpression {
        checkNotNull(filter)
        val conditions = filter.entries.map { (name, condition) ->
            condition.toConditionExpression(ConditionExpression.Parameter.Attribute(QueryAttribute.Simple.Value(name)))
        }
        return conditions.reduce { expr, cond ->
            when (conditionalOperator) {
                ConditionalOperator.AND -> ConditionExpression.And(expr, cond)
                ConditionalOperator.OR -> ConditionExpression.Or(expr, cond)
                else -> throw dynamoException("Unknown conditional operator to combine query filter")
            }
        }
    }

    fun retrieveKeyConditions(): Pair<Map<String, Condition>?, Set<String>> {
        if (!hasKeyConditions) {
            check(keyConditions == null && keyConditionExpression == null)
            return null to emptySet()
        }
        return if (keyConditions != null) keyConditions to emptySet()
        else {
            if (keyConditionExpression == null) {
                throw dynamoException("Nor key conditions, nor key condition expression was provided.")
            }
            val expressionGrammar = ConditionExpressionGrammar(expressionAttributeNames, expressionAttributeValues)
            when (val conditionExpression = expressionGrammar.parse(keyConditionExpression)) {
                is ConditionExpression.And -> mapOf(
                    conditionExpression.left.toKeyCondition(),
                    conditionExpression.right.toKeyCondition()
                ) to expressionGrammar.getUsedExpressionAttributes()
                else -> {
                    val used = expressionGrammar.getUsedExpressionAttributes()
                    mapOf(conditionExpression.toKeyCondition()) to used
                }
            }
        }
    }

    fun retrieveAttributesTransformer(
        table: TableMetadata
    ): (Map<String, AttributeValue>) -> (Map<String, AttributeValue>) {
        val select = select ?: if (indexName == null) null else Select.ALL_PROJECTED_ATTRIBUTES

        if (select == Select.ALL_ATTRIBUTES || select == Select.COUNT) {
            dynamoRequires(projectionExpression == null && attributesToGet == null) {
                "ProjectionExpression and AttributesToGet must be null if Select != SPECIFIC_ATTRIBUTES"
            }
            return { it }
        }
        if (select == Select.ALL_PROJECTED_ATTRIBUTES) {
            if (indexName == null) {
                throw dynamoException("Select cannot be 'ALL_PROJECTED_ATTRIBUTES' without provided index.")
            }
            dynamoRequires(projectionExpression == null && attributesToGet == null) {
                "ProjectionExpression and AttributesToGet must be null if select is 'ALL_PROJECTED_ATTRIBUTES'."
            }
            val index = table.localSecondaryIndex(indexName)
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
                val grammar = ProjectionExpressionGrammar(expressionAttributeNames)
                val projection = grammar.parse(projectionExpression)
                return { attrs ->
                    projection.mapNotNull { attribute ->
                        val name = attribute.simpleName
                        attrs[name]?.let { value ->
                            attribute.retrieve(mapOf(name to value))?.let { name to value }
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
}

fun QueryRequest.toSelectRequest() = SelectRequest(
    tableName = tableName().also { dynamoRequires(it != null) },
    indexName = indexName(),
    attributesToGet = attributesToGet().takeIf { hasAttributesToGet() },
    limit = limit(),
    select = select(),
    keyConditions = keyConditions().takeIf { hasKeyConditions() },
    filter = queryFilter().takeIf { hasQueryFilter() },
    conditionalOperator = conditionalOperator(),
    scanIndexForward = scanIndexForward() ?: true,
    exclusiveStartKey = exclusiveStartKey().takeIf { hasExclusiveStartKey() },
    returnConsumedCapacity = returnConsumedCapacity(),
    projectionExpression = projectionExpression(),
    filterExpression = filterExpression(),
    keyConditionExpression = keyConditionExpression(),
    expressionAttributeNames = expressionAttributeNames() ?: emptyMap(),
    expressionAttributeValues = expressionAttributeValues() ?: emptyMap(),
    consistentRead = consistentRead(),
    hasKeyConditions = true
)

fun ScanRequest.toSelectRequest() = SelectRequest(
    tableName = tableName().also { dynamoRequires(it != null) },
    indexName = indexName(),
    attributesToGet = attributesToGet().takeIf { hasAttributesToGet() },
    limit = limit(),
    select = select(),
    keyConditions = null,
    filter = scanFilter().takeIf { hasScanFilter() },
    conditionalOperator = conditionalOperator(),
    scanIndexForward = true,
    exclusiveStartKey = exclusiveStartKey().takeIf { hasExclusiveStartKey() },
    returnConsumedCapacity = returnConsumedCapacity(),
    projectionExpression = projectionExpression(),
    filterExpression = filterExpression(),
    keyConditionExpression = null,
    expressionAttributeNames = expressionAttributeNames() ?: mapOf(),
    expressionAttributeValues = expressionAttributeValues() ?: mapOf(),
    consistentRead = consistentRead(),
    hasKeyConditions = false
)
