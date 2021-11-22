package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.QueryAttribute
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

// Query Attributes

internal fun v(name: String) = QueryAttribute.Simple.Value(name)

internal fun mv(attribute: QueryAttribute.Simple, value: QueryAttribute) = QueryAttribute.MapValue(attribute, value)

internal fun lv(attribute: QueryAttribute.Simple, index: Int) = QueryAttribute.Simple.ListValue(attribute, index)

// Attribute Values

internal fun atS(value: String) = AttributeValue.builder().s(value).build()

internal fun atN(value: String) = AttributeValue.builder().n(value).build()

internal fun atSS(vararg values: String) = AttributeValue.builder().ss(values.toList()).build()

internal fun atM(vararg kv: Pair<String, AttributeValue>) = AttributeValue.builder().m(kv.toMap()).build()
