package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.QueryAttribute
import ru.hse.dynamomock.parser.Grammar
import ru.hse.dynamomock.parser.combinators.separated
import ru.hse.dynamomock.parser.lexer.LiteralToken

internal class ProjectionExpressionGrammar(
    expressionAttributeNames: Map<String, String>
) : Grammar<List<QueryAttribute>>() {
    private val comma by LiteralToken(",")

    private val attributeGrammar = QueryAttributeGrammar(expressionAttributeNames).also {
        externalGrammar(it)
    }

    override val parser by separated(attributeGrammar.parser, comma)
}
