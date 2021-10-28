package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*

internal class MapCombinator<T, R>(
    private val parser: OrdinaryParser<T>,
    private val transform: (T) -> R
) : OrdinaryParser<R> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<R> {
        return when (val result = parser.parse(tokens, fromIndex)) {
            is SuccessfulParse -> ParsedValue(transform(result.value), result.nextPosition)
            is FailedParse -> result
        }
    }
}

internal infix fun <T, R> OrdinaryParser<T>.map(transform: (T) -> R): OrdinaryParser<R> =
    MapCombinator(this, transform)

internal operator fun <R> OrdinaryParser<ParsedToken>.invoke(transform: (ParsedToken) -> R): OrdinaryParser<R> =
    MapCombinator(this, transform)
