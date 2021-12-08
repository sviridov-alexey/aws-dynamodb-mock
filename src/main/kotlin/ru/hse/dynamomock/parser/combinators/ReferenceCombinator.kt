package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.OrdinaryParser
import ru.hse.dynamomock.parser.ParseResult
import ru.hse.dynamomock.parser.ParsedToken
import ru.hse.dynamomock.parser.Parser

internal class ReferenceCombinator<out T>(getParser: () -> Parser<T>): OrdinaryParser<T> {
    private val parser by lazy(getParser)

    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T> =
        parser.parse(tokens, fromIndex)
}

internal fun <T> ref(getParser: () -> OrdinaryParser<T>): OrdinaryParser<T> =
    ReferenceCombinator(getParser)

internal fun <T> ref(getParser: () -> SkipCombinator<T>): SkipCombinator<T> =
    SkipCombinator(ReferenceCombinator(getParser))
