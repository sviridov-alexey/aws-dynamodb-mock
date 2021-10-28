package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*
import ru.hse.dynamomock.parser.ParseResult
import ru.hse.dynamomock.parser.ParsedToken
import ru.hse.dynamomock.parser.Parser
import ru.hse.dynamomock.parser.SuccessfulParse

internal class SeparatedCombinator<T>(
    private val item: Parser<T>,
    private val separator: Parser<*>,
    private val allowEmpty: Boolean
): OrdinaryParser<List<T>> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<List<T>> {
        val values = mutableListOf<T>()
        var nextPosition = fromIndex
        while (true) {
            when (val itemResult = item.parse(tokens, nextPosition)) {
                is SuccessfulParse -> {
                    values += itemResult.value
                    when (val sepResult = separator.parse(tokens, itemResult.nextPosition)) {
                        is SuccessfulParse -> nextPosition = sepResult.nextPosition
                        is FailedParse -> return ParsedValue(values, itemResult.nextPosition)
                    }
                }
                is FailedParse -> {
                    return if (values.isEmpty() && !allowEmpty) itemResult else ParsedValue(values, nextPosition)
                }
            }
        }
    }
}

internal fun <T> separated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    allowEmpty: Boolean = false
): OrdinaryParser<List<T>> = SeparatedCombinator(item, separator, allowEmpty)

internal fun <T> separated(
    item: SkipCombinator<T>,
    separator: Parser<*>,
    allowEmpty: Boolean = false
): SkipCombinator<List<T>> = SkipCombinator(SeparatedCombinator(item, separator, allowEmpty))

internal fun <T : R, R> leftAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    transform: (R, T) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = false) map { it.reduce(transform) }

internal fun <T, R> leftAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    init: R,
    transform: (R, T) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = true) map { it.fold(init, transform) }

internal fun <T : R, R> leftAssociated(
    item: OrdinaryParser<T>,
    transform: (R, T) -> R
): OrdinaryParser<R> = leftAssociated(item, EmptyCombinator, transform)

internal fun <T, R> leftAssociated(
    item: OrdinaryParser<T>,
    init: R,
    transform: (R, T) -> R
): OrdinaryParser<R> = leftAssociated(item, EmptyCombinator, init, transform)

internal fun <T : R, R> rightAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    transform: (T, R) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = false) map { it.reduceRight(transform) }

internal fun <T, R> rightAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    init: R,
    transform: (T, R) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = true) map { it.foldRight(init, transform) }

internal fun <T : R, R> rightAssociated(
    item: OrdinaryParser<T>,
    transform: (T, R) -> R
): OrdinaryParser<R> = rightAssociated(item, EmptyCombinator, transform)

internal fun <T, R> rightAssociated(
    item: OrdinaryParser<T>,
    init: R,
    transform: (T, R) -> R
): OrdinaryParser<R> = rightAssociated(item, EmptyCombinator, init, transform)
