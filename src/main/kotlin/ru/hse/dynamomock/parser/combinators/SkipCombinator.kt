package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*

internal class SkipCombinator(private val parser: OrdinaryParser<*>) : Parser<Unit> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<Unit> =
        when (val result = parser.parse(tokens, fromIndex)) {
            is SuccessfulParse -> ParsedValue(Unit, result.nextPosition)
            is FailedParse -> result
        }
}

internal fun skip(parser: OrdinaryParser<*>): SkipCombinator = SkipCombinator(parser)

internal operator fun <T> OrdinaryParser<T>.unaryMinus(): SkipCombinator = skip(this)
