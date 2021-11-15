package ru.hse.dynamomock.model.query

import ru.hse.dynamomock.model.AttributeTypeInfo
import ru.hse.dynamomock.model.toAttributeTypeInfo
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

// TODO support size()
sealed class ConditionExpression {
    // TODO consider maps and lists
    sealed interface Parameter {
        class Name(val name: String) : Parameter
        class Value(val value: AttributeValue) : Parameter
    }

    abstract fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean

    protected fun getValue(
        parameter: Parameter,
        attributeValues: Map<String, AttributeValue>,
    ) = when (parameter) {
        is Parameter.Name -> attributeValues.getValue(parameter.name)
        is Parameter.Value -> parameter.value
    }

    class And(private val left: ConditionExpression, private val right: ConditionExpression) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            left.evaluate(attributeValues) && right.evaluate(attributeValues)
    }

    class Or(private val left: ConditionExpression, private val right: ConditionExpression) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            left.evaluate(attributeValues) || right.evaluate(attributeValues)
    }

    class Not(private val expression: ConditionExpression) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) = !expression.evaluate(attributeValues)
    }

    abstract class Condition(
        private val leftParam: Parameter,
        private val rightParam: Parameter
    ) : ConditionExpression() {
        abstract fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo): Boolean

        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val leftValue = getValue(leftParam, attributeValues)
            val rightValue = getValue(rightParam, attributeValues)
            val leftTypeInfo = leftValue.toAttributeTypeInfo()
            val rightTypeInfo = rightValue.toAttributeTypeInfo()
            if (leftTypeInfo.typeAsString != rightTypeInfo.typeAsString) {
                return false
            }
            return compare(leftTypeInfo, rightTypeInfo)
        }
    }

    class Eq(leftParam: Parameter, rightParam: Parameter) : Condition(leftParam, rightParam) {
        override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = left.value == right.value
    }

    class Neq(leftParam: Parameter, rightParam: Parameter) : Condition(leftParam, rightParam) {
        override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = left.value != right.value
    }

    abstract class ConditionWithComparable(
        leftParam: Parameter,
        rightParam: Parameter
    ) : Condition(leftParam, rightParam) {
        abstract fun <T : Comparable<T>> compare(left: T, right: T): Boolean

        final override fun compare(left: AttributeTypeInfo, right: AttributeTypeInfo) = when (left.typeAsString) {
            "S" -> compare(left.value as String, right.value as String)
            "N" -> compare((left.value as String).toBigDecimal(), (right.value as String).toBigDecimal())
            "B" -> TODO()
            else -> false
        }
    }

    class Le(leftParam: Parameter, rightParam: Parameter) : ConditionWithComparable(leftParam, rightParam) {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left <= right
    }

    class Lt(leftParam: Parameter, rightParam: Parameter) : ConditionWithComparable(leftParam, rightParam) {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left < right
    }

    class Ge(leftParam: Parameter, rightParam: Parameter) : ConditionWithComparable(leftParam, rightParam) {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left >= right
    }

    class Gt(leftParam: Parameter, rightParam: Parameter) : ConditionWithComparable(leftParam, rightParam) {
        override fun <T : Comparable<T>> compare(left: T, right: T) = left > right
    }

    class Between(
        private val param: Parameter,
        private val leftParam: Parameter,
        private val rightParam: Parameter
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            And(Le(leftParam, param), Le(param, rightParam)).evaluate(attributeValues)
    }

    class In(
        private val attr: Parameter,
        private val array: List<Parameter>
    ) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) =
            array.any { Eq(attr, it).evaluate(attributeValues) }
    }

    class AttributeExists(private val attr: Parameter.Name) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>) = attr.name in attributeValues
    }

    class AttributeType(private val attr: Parameter.Name, private val type: Parameter.Value) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val typeInfo = type.value.toAttributeTypeInfo()
            require(typeInfo.typeAsString == "S")
            // TODO check if there is an existent type in [type]
            return getValue(attr, attributeValues).toAttributeTypeInfo().typeAsString == typeInfo.value
        }
    }

    class BeginsWith(private val attr: Parameter.Name, private val start: Parameter.Value) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val attrTypeInfo = getValue(attr, attributeValues).toAttributeTypeInfo()
            val startTypeInfo = start.value.toAttributeTypeInfo()
            require(attrTypeInfo.typeAsString == "S" || attrTypeInfo.typeAsString == "B")
            require(startTypeInfo.typeAsString == "S")
            // TODO support B in [attr]
            return (attrTypeInfo.value as String).startsWith(startTypeInfo.value as String)
        }
    }

    class Contains(private val attr: Parameter.Name, private val operand: Parameter.Value) : ConditionExpression() {
        override fun evaluate(attributeValues: Map<String, AttributeValue>): Boolean {
            val attrTypeInfo = getValue(attr, attributeValues).toAttributeTypeInfo()
            val operandTypeInfo = operand.value.toAttributeTypeInfo()
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
