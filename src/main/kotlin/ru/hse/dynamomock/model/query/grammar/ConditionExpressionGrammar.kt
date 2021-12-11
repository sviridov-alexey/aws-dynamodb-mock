package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.ConditionExpression
import ru.hse.dynamomock.model.query.ConditionExpression.Parameter
import ru.hse.dynamomock.parser.Grammar
import ru.hse.dynamomock.parser.OrdinaryParser
import ru.hse.dynamomock.parser.combinators.*
import ru.hse.dynamomock.parser.combinators.map
import ru.hse.dynamomock.parser.combinators.or
import ru.hse.dynamomock.parser.combinators.separated
import ru.hse.dynamomock.parser.lexer.LiteralToken
import ru.hse.dynamomock.parser.lexer.RegexToken
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal class ConditionExpressionGrammar(
    expressionAttributeNames: Map<String, String>,
    private val expressionAttributeValues: Map<String, AttributeValue>
) : Grammar<ConditionExpression>() {
    private val lp by LiteralToken("(")
    private val rp by LiteralToken(")")
    private val comma by LiteralToken(",")

    private val eq by LiteralToken("=")
    private val neq by LiteralToken("<>")
    private val le by LiteralToken("<=")
    private val lt by LiteralToken("<")
    private val ge by LiteralToken(">")
    private val gt by LiteralToken(">=")

    private val andKw by LiteralToken("and", ignoreCase = true)
    private val orKw by LiteralToken("or", ignoreCase = true)
    private val notKw by LiteralToken("not", ignoreCase = true)
    private val betweenKw by LiteralToken("between", ignoreCase = true)
    private val inKw by LiteralToken("in", ignoreCase = true)

    private val attributeExistsKw by LiteralToken("attribute_exists")
    private val attributeNotExistsKw by LiteralToken("attribute_not_exists")
    private val attributeTypeKw by LiteralToken("attribute_type")
    private val beginsWithKw by LiteralToken("begins_with")
    private val containsKw by LiteralToken("contains")
    private val sizeKw by LiteralToken("size")

    private val valueT by RegexToken(":[a-zA-Z0-9]+")

    @Suppress("unused")
    private val ws by RegexToken("\\s+", ignore = true)

    private val attributeGrammar by QueryAttributeGrammar(expressionAttributeNames)
    private val attribute by attributeGrammar.parser map { Parameter.Attribute(it) }

    private fun <T> mayParens(parser: OrdinaryParser<T>): OrdinaryParser<T> = parser or -lp * parser * -rp

    private fun <T> function1(name: OrdinaryParser<*>, arg: OrdinaryParser<T>) = -name * -lp * mayParens(arg) * -rp

    private fun <T, R> function2(name: OrdinaryParser<*>, arg1: OrdinaryParser<T>, arg2: OrdinaryParser<R>) =
        -name * -lp * mayParens(arg1) * -comma * mayParens(arg2) * -rp

    private val size by function1(sizeKw, attributeGrammar.parser) map { Parameter.AttributeSize(it) }
    private val value by valueT map { Parameter.Value(expressionAttributeValues.getValue(it.text)) }
    private val operand by mayParens(attribute or value or size)

    private val conditionOp by (eq or neq or le or lt or gt or ge) map { it.text }
    private val condition by operand * conditionOp * operand map { (rest, right) ->
        val (left, op) = rest
        when (op) {
            "="  -> ConditionExpression.Eq(left, right)
            "<>" -> ConditionExpression.Neq(left, right)
            "<=" -> ConditionExpression.Le(left, right)
            "<"  -> ConditionExpression.Lt(left, right)
            ">=" -> ConditionExpression.Ge(left, right)
            ">"  -> ConditionExpression.Gt(left, right)
            else -> throw IllegalStateException("Unexpected operator in primary expression.")
        }
    }

    private val attributeExistsF by function1(attributeExistsKw, attribute) map {
        ConditionExpression.AttributeExists(it)
    }
    private val attributeNotExistsF by function1(attributeNotExistsKw, attribute) map {
        ConditionExpression.Not(ConditionExpression.AttributeExists(it))
    }
    private val attributeTypeF by function2(attributeTypeKw, attribute, value) map { (attr, type) ->
        ConditionExpression.AttributeType(attr, type)
    }
    private val beginsWithF by function2(beginsWithKw, attribute, attribute or value) map { (attr, substring) ->
        ConditionExpression.BeginsWith(attr, substring)
    }
    private val containsF by function2(containsKw, attribute, attribute or value or size) map { (attr, op) ->
        ConditionExpression.Contains(attr, op)
    }
    private val betweenF by operand * -betweenKw * operand * -andKw * operand map {
        ConditionExpression.Between(it.first.first, it.first.second, it.second)
    }
    private val inF by operand * -inKw * -lp * separated(operand, comma) * -rp map { (attr, list) ->
        ConditionExpression.In(attr, list)
    }

    private val function by betweenF or inF or attributeExistsF or attributeNotExistsF or
            attributeTypeF or beginsWithF or containsF

    private val primary: OrdinaryParser<ConditionExpression> by condition or function or
            (-lp * ref(this::parser) * -rp) or (-notKw * ref(this::primary) map { ConditionExpression.Not(it) })

    private val ands by leftAssociated(primary, andKw) { l, r, _ -> ConditionExpression.And(l, r) }

    override val parser by leftAssociated(ands, orKw) { l, r, _ -> ConditionExpression.Or(l, r) }
}
