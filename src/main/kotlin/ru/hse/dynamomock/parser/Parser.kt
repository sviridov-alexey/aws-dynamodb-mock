/**
 * Inspired by `better-parse`-library with `h0tk3y` as the author.
 * Link to the original project: https://github.com/h0tk3y/better-parse
 *
 * Thanks for that wonderful project!
 */

package ru.hse.dynamomock.parser

import ru.hse.dynamomock.parser.combinators.and
import ru.hse.dynamomock.parser.combinators.eof

internal interface Parser<out T> {
    fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T>
}

internal interface OrdinaryParser<out T> : Parser<T>

internal fun <T> OrdinaryParser<T>.parseToEnd(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T> =
    (this and eof).parse(tokens, fromIndex)
