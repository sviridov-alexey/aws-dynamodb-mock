package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*

internal val eof = skip(object : OrdinaryParser<Unit> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<Unit> = when (fromIndex) {
        tokens.size -> ParsedValue(Unit, fromIndex)
        else -> IncompleteParseFail(tokens[fromIndex].token)
    }
})
