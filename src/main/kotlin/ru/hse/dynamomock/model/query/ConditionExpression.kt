package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.AttributeTypeInfo
import ru.hse.dynamomock.model.toAttributeTypeInfo
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

// TODO support B and BS in some methods
sealed class ConditionExpression {
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

    abstract fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean

    data class And(
        private val left: ConditionExpression,
        private val right: ConditionExpression
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            left.evaluate(attributeValues) && right.evaluate(attributeValues)
    }

    data class Or(
        private val left: ConditionExpression,
        private val right: ConditionExpression
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            left.evaluate(attributeValues) || right.evaluate(attributeValues)
    }

    data class Not(private val expression: ConditionExpression) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) = !expression.evaluate(attributeValues)
    }

    abstract class Condition : ConditionExpression() {
        protected abstract val leftParam: Parameter
        protected abstract val rightParam: Parameter

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
    ) : Condition() {
        override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = left.value == right.value
    }

    data class Neq(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : Condition() {
        override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = left.value != right.value
    }

    abstract class ConditionWithComparable : Condition() {
        abstract fun <T : Comparable<T>> compare(left: T, right: T): Boolean

        final override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = when (left.typeAsString) {
            "S" -> compare(left.value as String, right.value as String)
            "N" -> compare((left.value as String).toBigDecimal(), (right.value as String).toBigDecimal())
            "B" -> TODO()
            else -> false
        }
    }

    data class Le(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : ConditionWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left <= right
    }

    data class Lt(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    ) : ConditionWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left < right
    }

    data class Ge(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    )  : ConditionWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left >= right
    }

    data class Gt(
        override val leftParam: Parameter,
        override val rightParam: Parameter
    )  : ConditionWithComparable() {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left > right
    }

    data class Between(
        private val param: Parameter,
        private val leftParam: Parameter,
        private val rightParam: Parameter
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            And(Le(leftParam, param), Le(param, rightParam)).evaluate(attributeValues)
    }

    data class In(
        private val attr: Parameter,
        private val list: List<Parameter>
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            list.any { Eq(attr, it).evaluate(attributeValues) }
    }

    data class AttributeExists(private val attr: Parameter.Attribute) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            attr.attribute.retrieve(attributeValues) != null
    }

    data class AttributeType(
        private val attr: Parameter.Attribute,
        private val type: Parameter.Value
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val typeInfo = type.value.toAttributeTypeInfo()
            require(typeInfo.typeAsString == "S")
            // TODO check if there is an existent type in [type]
            return attr.retrieve(attributeValues)?.toAttributeTypeInfo()?.typeAsString == typeInfo.value
        }
    }

    data class BeginsWith(
        private val attr: Parameter.Attribute,
        private val start: Parameter
    ) : ConditionExpression() {
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
        private val attr: Parameter.Attribute,
        private val operand: Parameter
    ) : ConditionExpression() {
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
