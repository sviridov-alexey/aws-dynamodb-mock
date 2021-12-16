package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
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
                    attributeValue.hasNs() -> attributeValue.ns().size
                    attributeValue.hasBs() -> attributeValue.bs().size
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

        final override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo): Boolean {
            if (left.typeAsString != right.typeAsString) {
                return false
            }
            return when (left.typeAsString) {
                "S" -> compare(left.value as String, right.value as String)
                "N" -> compare(left.value as BigDecimal, right.value as BigDecimal)
                "B" -> TODO()
                else -> false
            }
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
            dynamoRequires(list.size <= 100) {
                "List in `in` operator must contain up to 100 values."
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
            dynamoRequires(typeInfo.typeAsString == "S") {
                "Operand in 'attribute_type' must be S."
            }
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
            dynamoRequires(attrTypeInfo.typeAsString == "S" || attrTypeInfo.typeAsString == "B") {
                "Operands in 'begins_with' must be S."
            }
            dynamoRequires(startTypeInfo.typeAsString == "S") {
                "Operands in 'begins_with' must be S."
            }
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
            val type = attrTypeInfo.typeAsString
            return when {
                type == "S" -> {
                    dynamoRequires(operandTypeInfo.typeAsString == "S") {
                        "Operand in 'contains(S, ...)' must be S."
                    }
                    (attrTypeInfo.value as String).contains(operandTypeInfo.value as String)
                }
                type.length == 2 && type.endsWith('S') -> {
                    if (type.first().toString() == operandTypeInfo.typeAsString) {
                        (attrTypeInfo.value as Set<*>).contains(operandTypeInfo.value)
                    } else {
                        false
                    }
                }
                else -> false
            }
        }
    }
}

fun Condition.toConditionExpression(attribute: Parameter.Attribute): ConditionExpression {
    val arguments = attributeValueList().map { Parameter.Value(it) }

    fun assertArgs(number: Int, strict: Boolean = true) {
        dynamoRequires(strict && number == arguments.size || !strict && number >= arguments.size) {
            "Invalid number of arguments in ${comparisonOperator()}."
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
        else -> throw dynamoException("Unknown comparison operator in filter expression")
    }
}

private fun QueryAttribute.toValue(): QueryAttribute.Simple.Value = this as? QueryAttribute.Simple.Value
    ?: throw dynamoException("Attribute in key comparison must have type from [N, S, B]")

fun ConditionExpression.toKeyCondition(): Pair<String, Condition> = when (this) {
    is ConditionExpression.Neq -> throw dynamoException(
        "Comparison in key condition expression cannot have <> operator."
    )
    is ConditionExpression.Comparison -> {
        val attributeParameter = leftParam as? Parameter.Attribute
            ?: rightParam as? Parameter.Attribute
            ?: throw dynamoException("Comparison in key condition expression must contain exactly one attribute.")
        val attribute = attributeParameter.attribute.toValue()
        val valueParameter = leftParam as? Parameter.Value
            ?: rightParam as? Parameter.Value
            ?: throw dynamoException("Comparison in key condition expression must contain exactly one value.")

        attribute.name to Condition.builder()
            .comparisonOperator(this::class.simpleName!!.uppercase())
            .attributeValueList(valueParameter.value)
            .build()
    }
    is ConditionExpression.BeginsWith -> {
        val attribute = attr.attribute.toValue()
        val valueParameter = start as? Parameter.Value
            ?: throw dynamoException("BeginsWith in key condition expression must have value as the second parameter.")

        attribute.name to Condition.builder()
            .comparisonOperator(ComparisonOperator.BEGINS_WITH)
            .attributeValueList(valueParameter.value)
            .build()
    }
    is ConditionExpression.Between -> {
        val attribute = (param as? Parameter.Attribute)?.attribute?.toValue() ?: throw dynamoException(
            "Between in key condition expression must have simple attribute as the first parameter."
        )
        val valueParameters = listOf(leftParam, rightParam).map {
            it as? Parameter.Value ?: throw dynamoException(
                "Between in key condition expression must have values as the second and third parameters"
            )
        }

        attribute.name to Condition.builder()
            .comparisonOperator(ComparisonOperator.BETWEEN)
            .attributeValueList(valueParameters.map { it.value })
            .build()
    }
    else -> throw dynamoException("Unsupported operation in key condition expression")
}
