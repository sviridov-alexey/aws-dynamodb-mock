package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*

internal class SkipCombinator<T>(private val parser: OrdinaryParser<T>) : Parser<T> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T> =
        when (val result = parser.parse(tokens, fromIndex)) {
            is SuccessfulParse -> ParsedValue(result.value, result.nextPosition)
            is FailedParse -> result
        }

    internal operator fun unaryPlus(): OrdinaryParser<T> = parser
}

internal fun <T> skip(parser: OrdinaryParser<T>): SkipCombinator<T> = SkipCombinator(parser)

internal operator fun <T> OrdinaryParser<T>.unaryMinus(): SkipCombinator<T> = skip(this)
