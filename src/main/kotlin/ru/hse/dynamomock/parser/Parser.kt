/**
 * Inspired by `better-parse`-library with `h0tk3y` as the author.
 * Link to the original project: https://github.com/h0tk3y/better-parse
 *
 * Thanks for that wonderful project!
 */

package ru.hse.dynamomock.parser

internal interface Parser<out T> {
    fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T>
}

internal interface OrdinaryParser<out T> : Parser<T>

internal fun <T> Parser<T>.parseToEnd(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T> =
    when (val result = parse(tokens, fromIndex)) {
        is FailedParse -> result
        is SuccessfulParse -> when (val position = result.nextPosition) {
            tokens.size -> result
            else -> IncompleteParseFail(tokens[position].token)
        }
    }
