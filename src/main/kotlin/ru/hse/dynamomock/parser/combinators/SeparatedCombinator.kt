package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*
import ru.hse.dynamomock.parser.ParseResult
import ru.hse.dynamomock.parser.ParsedToken
import ru.hse.dynamomock.parser.Parser
import ru.hse.dynamomock.parser.SuccessfulParse

internal class SeparatedCombinator<T, S>(
    private val item: Parser<T>,
    private val separator: Parser<S>,
    private val allowEmpty: Boolean
): OrdinaryParser<Pair<List<T>, List<S>>> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<Pair<List<T>, List<S>>> {
        val items = mutableListOf<T>()
        val separators = mutableListOf<S>()
        var nextPosition = fromIndex
        var returnPosition = fromIndex
        while (true) {
            when (val itemResult = item.parse(tokens, nextPosition)) {
                is SuccessfulParse -> {
                    items += itemResult.value
                    returnPosition = itemResult.nextPosition
                    when (val separatorResult = separator.parse(tokens, itemResult.nextPosition)) {
                        is SuccessfulParse -> {
                            separators += separatorResult.value
                            nextPosition = separatorResult.nextPosition
                        }
                        is FailedParse -> return ParsedValue(items to separators, itemResult.nextPosition)
                    }
                }
                is FailedParse -> {
                    return if (items.isEmpty() && !allowEmpty) itemResult else {
                        if (separators.isNotEmpty()) {
                            separators.removeLast()
                        }
                        ParsedValue(items to separators, returnPosition)
                    }
                }
            }
        }
    }
}

internal fun <T> separated(
    item: OrdinaryParser<T>,
    separator: Parser<*>,
    allowEmpty: Boolean = false
): OrdinaryParser<List<T>> = SeparatedCombinator(item, separator, allowEmpty) map { it.first }

internal fun <T> separated(
    item: SkipCombinator<T>,
    separator: Parser<*>,
    allowEmpty: Boolean = false
): SkipCombinator<List<T>> = SkipCombinator(SeparatedCombinator(item, separator, allowEmpty) map { it.first })

internal fun <T : R, S, R> leftAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<S>,
    transform: (R, T, S) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = false) map { (items, separators) ->
    var result: R = items.first()
    for (i in 1 until items.size) {
        result = transform(result, items[i], separators[i - 1])
    }
    result
}

internal fun <T : R, R> leftAssociated(
    item: OrdinaryParser<T>,
    transform: (R, T, Unit) -> R
): OrdinaryParser<R> = leftAssociated(item, EmptyCombinator, transform)

internal fun <T : R, R> leftAssociated(
    item: OrdinaryParser<T>,
    transform: (R, T) -> R
): OrdinaryParser<R> = leftAssociated(item) { r: R, i: T, _ -> transform(r, i) }

internal fun <T : R, S, R> rightAssociated(
    item: OrdinaryParser<T>,
    separator: Parser<S>,
    transform: (T, S, R) -> R
): OrdinaryParser<R> = SeparatedCombinator(item, separator, allowEmpty = false) map { (items, separators) ->
    var result: R = items.last()
    for (i in items.size - 2 downTo 0) {
        result = transform(items[i], separators[i], result)
    }
    result
}

internal fun <T : R, R> rightAssociated(
    item: OrdinaryParser<T>,
    transform: (T, Unit, R) -> R
): OrdinaryParser<R> = rightAssociated(item, EmptyCombinator, transform)

internal fun <T : R, R> rightAssociated(
    item: OrdinaryParser<T>,
    transform: (T, R) -> R
): OrdinaryParser<R> = rightAssociated(item, EmptyCombinator) { i: T, _, r: R -> transform(i, r) }
