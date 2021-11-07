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
        var returnPosition = fromIndex
        while (true) {
            when (val itemResult = item.parse(tokens, nextPosition)) {
                is SuccessfulParse -> {
                    values += itemResult.value
                    returnPosition = itemResult.nextPosition
                    when (val separatorResult = separator.parse(tokens, itemResult.nextPosition)) {
                        is SuccessfulParse -> nextPosition = separatorResult.nextPosition
                        is FailedParse -> return ParsedValue(values, itemResult.nextPosition)
                    }
                }
                is FailedParse -> {
                    return if (values.isEmpty() && !allowEmpty) itemResult else ParsedValue(values, returnPosition)
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

internal fun <T : R, R> leftAssociated(
    item: OrdinaryParser<T>,
    transform: (R, T) -> R
): OrdinaryParser<R> = leftAssociated(item, EmptyCombinator, transform)

internal fun <T : R, R> rightAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    transform: (T, R) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = false) map { it.reduceRight(transform) }

internal fun <T : R, R> rightAssociated(
    item: OrdinaryParser<T>,
    transform: (T, R) -> R
): OrdinaryParser<R> = rightAssociated(item, EmptyCombinator, transform)
