package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*

internal class AndCombinator<T, S, R>(
    private val first: Parser<T>,
    private val second: Parser<S>,
    private val transform: (T, S) -> R
) : OrdinaryParser<R> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<R> {
        return when (val firstResult = first.parse(tokens, fromIndex)) {
            is FailedParse -> firstResult
            is SuccessfulParse -> {
                return when (val secondResult = second.parse(tokens, firstResult.nextPosition)) {
                    is FailedParse -> secondResult
                    is SuccessfulParse -> ParsedValue(
                        transform(firstResult.value, secondResult.value),
                        secondResult.nextPosition
                    )
                }
            }
        }
    }
}

internal infix fun <T, S> SkipCombinator<T>.and(other: OrdinaryParser<S>): AndCombinator<T, S, S> =
    AndCombinator(this, other) { _, x -> x }

internal operator fun <T, S> SkipCombinator<T>.times(other: OrdinaryParser<S>) = this and other

internal infix fun <T, S> OrdinaryParser<T>.and(other: SkipCombinator<S>): AndCombinator<T, S, T> =
    AndCombinator(this, other) { x, _ -> x }

internal operator fun <T, S> OrdinaryParser<T>.times(other: SkipCombinator<S>) = this and other

internal infix fun <T, S> SkipCombinator<T>.and(other: SkipCombinator<S>): SkipCombinator<Pair<T, S>> =
    SkipCombinator(AndCombinator(this, other) { x, y -> x to y })

internal operator fun <T, S> SkipCombinator<T>.times(other: SkipCombinator<S>) = this and other

internal infix fun <T, S> OrdinaryParser<T>.and(other: OrdinaryParser<S>): OrdinaryParser<Pair<T, S>> =
    AndCombinator(this, other, ::Pair)

internal operator fun <T, S> OrdinaryParser<T>.times(other: OrdinaryParser<S>) = this and other
