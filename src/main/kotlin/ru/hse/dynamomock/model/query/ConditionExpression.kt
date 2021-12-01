package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.AttributeTypeInfo
import ru.hse.dynamomock.model.query.grammar.ConditionExpressionGrammar
import ru.hse.dynamomock.model.query.ConditionExpression.Parameter
import ru.hse.dynamomock.model.toAttributeTypeInfo
import software.amazon.awssdk.services.dynamodb.model.*
import java.math.BigDecimal

// TODO support B and BS in some methods
sealed interface ConditionExpression {
    // TODO maybe introduce interface for `retrieve`
    sealed interface Parameter {
        data class Attribute(val attribute: QueryAttribute) : Parameter
        data class Value(val value: AttributeValue) : Parameter
        data class AttributeSize(val attribute: QueryAttribute) : Parameter

        fun retrieve(attributeValues: Map<String, AttributeValue>): AttributeValue? = when (this) {
            is Attribute -> attribute.retrieve(attributeValues)
            is Value -> value
            is AttributeSize -> {
                val attributeValue = attribute.retrieve(attributeValues)
                when {
                    attributeValue == null -> null
                    attributeValue.s() != null -> attributeValue.s().length
                    attributeValue.b() != null -> attributeValue.b().asByteArray().size // TODO ???
                    attributeValue.hasSs() -> attributeValue.ss().size
                    attributeValue.hasL() -> attributeValue.l().size
                    attributeValue.hasM() -> attributeValue.m().size
                    else -> null
                }?.let { AttributeValue.builder().n(it.toString()).build() }
            }
        }
    }

    fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean

    data class And(
        val left: ConditionExpression,
        val right: ConditionExpression
    ) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            left.evaluate(attributeValues) && right.evaluate(attributeValues)
    }

    data class Or(
        val left: ConditionExpression,
        val right: ConditionExpression
    ) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            left.evaluate(attributeValues) || right.evaluate(attributeValues)
    }

    data class Not(val expression: ConditionExpression) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) = !expression.evaluate(attributeValues)
    }

    abstract class Comparison : ConditionExpression {
        abstract val leftParam: Parameter
        abstract val rightParam: Parameter

        protected abstract fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo): Boolean

        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val leftValue = leftParam.retrieve(attributeValues) ?: return false
            val rightValue = rightParam.retrieve(attributeValues) ?: return false
            val leftTypeInfo = leftValue.toAttributeTypeInfo()
            val rightTypeInfo = rightValue.toAttributeTypeInfo()
            if (leftTypeInfo.typeAsString != rightTypeInfo.typeAsString) {
                return false
            }
            return compare(leftTypeInfo, rightTypeInfo)
        }
    }

    data class Eq(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : Comparison() {
        override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = left.value == right.value
    }

    data class Neq(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : Comparison() {
        override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = left.value != right.value
    }

    abstract class ComparisonWithComparable : Comparison() {
        abstract fun <T : Comparable<T>> compare(left: T, right: T): Boolean

        final override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = when (left.typeAsString) {
            "S" -> compare(left.value as String, right.value as String)
            "N" -> compare(left.value as BigDecimal, right.value as BigDecimal)
            "B" -> TODO()
            else -> false
        }
    }

    data class Le(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : ComparisonWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left <= right
    }

    data class Lt(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : ComparisonWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left < right
    }

    data class Ge(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    )  : ComparisonWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left >= right
    }

    data class Gt(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    )  : ComparisonWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left > right
    }

    data class Between(
        val param: Parameter,
        val leftParam: Parameter,
        val rightParam: Parameter
    ) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            And(Le(leftParam, param), Le(param, rightParam)).evaluate(attributeValues)
    }

    data class In(
        val attr: Parameter,
        val list: List<Parameter>
    ) : ConditionExpression {
        init {
            if (list.size > 100) {
                throw DynamoDbException.builder()
                    .message("List in `in` operator must contain up to 100 values.")
                    .build()
            }
        }

        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            list.any { Eq(attr, it).evaluate(attributeValues) }
    }

    data class AttributeExists(val attr: Parameter.Attribute) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            attr.attribute.retrieve(attributeValues) != null
    }

    data class AttributeType(
        val attr: Parameter.Attribute,
        val type: Parameter.Value
    ) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val typeInfo = type.value.toAttributeTypeInfo()
            require(typeInfo.typeAsString == "S")
            // TODO check if there is an existent type in [type]
            return attr.retrieve(attributeValues)?.toAttributeTypeInfo()?.typeAsString == typeInfo.value
        }
    }

    data class BeginsWith(
        val attr: Parameter.Attribute,
        val start: Parameter
    ) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val attrTypeInfo = attr.retrieve(attributeValues)?.toAttributeTypeInfo() ?: return false
            val startTypeInfo = start.retrieve(attributeValues)?.toAttributeTypeInfo() ?: return false
            require(attrTypeInfo.typeAsString == "S" || attrTypeInfo.typeAsString == "B")
            require(startTypeInfo.typeAsString == "S")
            // TODO support B in [attr]
            return (attrTypeInfo.value as String).startsWith(startTypeInfo.value as String)
        }
    }

    data class Contains(
        val attr: Parameter.Attribute,
        val operand: Parameter
    ) : ConditionExpression {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val attrTypeInfo = attr.retrieve(attributeValues)?.toAttributeTypeInfo() ?: return false
            val operandTypeInfo = operand.retrieve(attributeValues)?.toAttributeTypeInfo() ?: return false
            return when (attrTypeInfo.typeAsString) {
                "S" -> {
                    require(operandTypeInfo.typeAsString == "S")
                    (attrTypeInfo.value as String).contains(operandTypeInfo.value as String)
                }
                "SS" -> (attrTypeInfo.value as List<*>).contains(operandTypeInfo.value)
                else -> throw IllegalArgumentException() // TODO
            }
        }
    }
}

fun QueryRequest.retrieveFilterExpression(): ConditionExpression? {
    return if (hasQueryFilter()) {
        retrieveConditionExpression(queryFilter(), conditionalOperator())
    } else if (filterExpression() != null) {
        val filterExpression = filterExpression()
        ConditionExpressionGrammar(expressionAttributeNames(), expressionAttributeValues()).parse(filterExpression)
    } else {
        null
    }
}

fun QueryRequest.retrieveKeyConditions(): Map<String, Condition> {
    return if (hasKeyConditions()) {
        keyConditions()
    } else {
        val keyConditionExpression = keyConditionExpression() ?: throw DynamoDbException.builder()
            .message("Nor key conditions, nor key condition expression was provided.")
            .build()
        val conditionExpression = ConditionExpressionGrammar(expressionAttributeNames(), expressionAttributeValues())
            .parse(keyConditionExpression)

        when (conditionExpression) {
            is ConditionExpression.And -> mapOf(
                conditionExpression.left.toKeyCondition(),
                conditionExpression.right.toKeyCondition()
            )
            else -> mapOf(conditionExpression.toKeyCondition())
        }
    }
}

private fun retrieveConditionExpression(
    filter: Map<String, Condition>,
    conditionalOperator: ConditionalOperator
): ConditionExpression {
    val conditions = filter.entries.map { (name, condition) ->
        condition.toConditionExpression(Parameter.Attribute(QueryAttribute.Simple.Value(name)))
    }
    return conditions.reduce { expr, cond ->
        when (conditionalOperator) {
            ConditionalOperator.AND -> ConditionExpression.And(expr, cond)
            ConditionalOperator.OR -> ConditionExpression.Or(expr, cond)
            else -> throw DynamoDbException.builder()
                .message("Unknown conditional operator to combine query filter")
                .build()
        }
    }
}

private fun Condition.toConditionExpression(attribute: Parameter.Attribute): ConditionExpression {
    val arguments = attributeValueList().map { Parameter.Value(it) }

    fun assertArgs(number: Int, strict: Boolean = true) {
        if (strict && number != arguments.size || !strict && number < arguments.size) {
            throw DynamoDbException.builder()
                .message("Invalid number of arguments in ${comparisonOperator()}.")
                .build()
        }
    }

    return when (comparisonOperator()) {
        ComparisonOperator.EQ -> {
            assertArgs(1)
            ConditionExpression.Eq(attribute, arguments.first())
        }
        ComparisonOperator.NE -> {
            assertArgs(1)
            ConditionExpression.Neq(attribute, arguments.first())
        }
        ComparisonOperator.LE -> {
            assertArgs(1)
            ConditionExpression.Le(attribute, arguments.first())
        }
        ComparisonOperator.LT -> {
            assertArgs(1)
            ConditionExpression.Lt(attribute, arguments.first())
        }
        ComparisonOperator.GE -> {
            assertArgs(1)
            ConditionExpression.Ge(attribute, arguments.first())
        }
        ComparisonOperator.GT -> {
            assertArgs(1)
            ConditionExpression.Gt(attribute, arguments.first())
        }
        ComparisonOperator.IN -> {
            assertArgs(1, strict = false)
            ConditionExpression.In(attribute, arguments)
        }
        ComparisonOperator.BETWEEN -> {
            assertArgs(2)
            ConditionExpression.Between(attribute, arguments[0], arguments[1])
        }
        ComparisonOperator.NOT_NULL -> {
            assertArgs(0)
            ConditionExpression.AttributeExists(attribute)
        }
        ComparisonOperator.NULL -> {
            assertArgs(0)
            ConditionExpression.Not(ConditionExpression.AttributeExists(attribute))
        }
        ComparisonOperator.CONTAINS -> {
            assertArgs(1)
            ConditionExpression.Contains(attribute, arguments.first())
        }
        ComparisonOperator.NOT_CONTAINS -> {
            assertArgs(1)
            ConditionExpression.Not(ConditionExpression.Contains(attribute, arguments.first()))
        }
        ComparisonOperator.BEGINS_WITH -> {
            assertArgs(1)
            ConditionExpression.BeginsWith(attribute, arguments.first())
        }
        else -> {
            throw DynamoDbException.builder().message("Unknown comparison operator in filter expression").build()
        }
    }
}

private fun QueryAttribute.toValue(): QueryAttribute.Simple.Value =
    this as? QueryAttribute.Simple.Value ?: throw DynamoDbException.builder()
        .message("Attribute in key comparison must have type from [N, S, B]").build()

private fun ConditionExpression.toKeyCondition(): Pair<String, Condition> = when (this) {
    is ConditionExpression.Neq -> {
        throw DynamoDbException.builder()
            .message("Comparison in key condition expression cannot have <> operator.").build()
    }
    is ConditionExpression.Comparison -> {
        val attributeParameter = leftParam as? Parameter.Attribute
            ?: rightParam as? Parameter.Attribute
            ?: throw DynamoDbException.builder()
                .message("Comparison in key condition expression must contain exactly one attribute.").build()
        val attribute = attributeParameter.attribute.toValue()
        val valueParameter = leftParam as? Parameter.Value
            ?: rightParam as? Parameter.Value
            ?: throw DynamoDbException.builder()
                .message("Comparison in key condition expression must contain exactly one value.").build()

        attribute.name to Condition.builder()
            .comparisonOperator(this::class.simpleName!!.uppercase())
            .attributeValueList(valueParameter.value)
            .build()
    }
    is ConditionExpression.BeginsWith -> {
        val attribute = attr.attribute.toValue()
        val valueParameter = start as? Parameter.Value ?: throw DynamoDbException.builder()
            .message("BeginsWith in key condition expression must have value as the second parameter.")
            .build()

        attribute.name to Condition.builder()
            .comparisonOperator(ComparisonOperator.BEGINS_WITH)
            .attributeValueList(valueParameter.value)
            .build()
    }
    is ConditionExpression.Between -> {
        val attribute = (param as? Parameter.Attribute)?.attribute?.toValue() ?: throw DynamoDbException.builder()
            .message("Between in key condition expression must have simple attribute as the first parameter.")
            .build()
        val valueParameters = listOf(leftParam, rightParam).map {
            it as? Parameter.Value ?: throw DynamoDbException.builder()
                .message("Between in key condition expression must have values as the second and third parameters")
                .build()
        }

        attribute.name to Condition.builder()
            .comparisonOperator(ComparisonOperator.BETWEEN)
            .attributeValueList(valueParameters.map { it.value })
            .build()
    }
    else -> throw DynamoDbException.builder().message("Unsupported operation in key condition expression").build()
}
