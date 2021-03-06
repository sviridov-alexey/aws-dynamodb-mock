package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.model.query.QueryAttribute
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

// Query Attributes

internal fun v(name: String) = QueryAttribute.Simple.Value(name)

internal fun mv(attribute: QueryAttribute.Simple, value: QueryAttribute) = QueryAttribute.MapValue(attribute, value)

internal fun lv(attribute: QueryAttribute.Simple, index: Int) = QueryAttribute.Simple.ListValue(attribute, index)

// Attribute Values

internal fun atS(value: String) = AttributeValue.builder().s(value).build()

private fun isNumeric(toCheck: String): Boolean {
    return toCheck.toDoubleOrNull() != null
}
internal fun atN(value: String) = if (isNumeric(value))
    AttributeValue.builder().n(value).build() else
    throw dynamoException("A value provided cannot be converted into a number $value")

internal fun atSS(vararg values: String) = AttributeValue.builder().ss(*values).build()

internal fun atNS(vararg values: String) = AttributeValue.builder().ns(*values).build()

internal fun atM(vararg kv: Pair<String, AttributeValue>) = AttributeValue.builder().m(kv.toMap()).build()

internal fun atL(vararg vs: AttributeValue) = AttributeValue.builder().l(*vs).build()
