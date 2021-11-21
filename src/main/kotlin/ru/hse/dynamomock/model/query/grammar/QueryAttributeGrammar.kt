package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.QueryAttribute
import ru.hse.dynamomock.parser.Grammar
import ru.hse.dynamomock.parser.combinators.*
import ru.hse.dynamomock.parser.lexer.LiteralToken
import ru.hse.dynamomock.parser.lexer.RegexToken

internal class QueryAttributeGrammar(
    private val expressionAttributeNames: Map<String, String>
) : Grammar<QueryAttribute>() {
    private val dot by LiteralToken(".")
    private val lbrace by LiteralToken("[")
    private val rbrace by LiteralToken("]")
    private val refName by RegexToken("#[a-zA-Z0-9]+")
    private val regularName by RegexToken("[a-zA-Z]([a-zA-Z0-9][a-zA-Z0-9_-]*)?")
    private val index by RegexToken("[0-9]+")

    @Suppress("unused")
    private val ws by RegexToken("\\s+", ignore = true)

    private val name by (refName { QueryAttribute.Simple.Value(expressionAttributeName(it.text.drop(1))) }) or
            (regularName map { QueryAttribute.Simple.Value(it.text) })

    private val attributePart by (name * separated(-lbrace * index * -rbrace, EmptyCombinator) map { (name, indices) ->
        indices.fold(name) { attr: QueryAttribute.Simple, ind ->
            QueryAttribute.Simple.ListValue(attr, ind.text.toInt())
        }
    }) or name

    override val parser by rightAssociated(attributePart, dot) { l, _, r: QueryAttribute ->
        QueryAttribute.MapValue(l, r)
    }

    // TODO ignore exceptions
    private fun expressionAttributeName(name: String) = requireNotNull(expressionAttributeNames[name]) {
        "Used undefined expression attribute name: '$name'."
    }
}
