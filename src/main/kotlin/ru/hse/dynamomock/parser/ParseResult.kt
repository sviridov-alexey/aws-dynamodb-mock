package ru.hse.dynamomock.parser

import ru.hse.dynamomock.parser.lexer.Token

internal sealed interface ParseResult<out T>

internal interface SuccessfulParse<out T> : ParseResult<T> {
    val value: T
    val nextPosition: Int
}

internal open class FailedParse(val message: String) : ParseResult<Nothing>

internal data class ParsedValue<out T>(override val value: T, override val nextPosition: Int) : SuccessfulParse<T>

internal data class ParsedToken(
    val token: Token,
    val tokenIndex: Int,
    val input: CharSequence,
    val offset: Int,
    val length: Int,
    val row: Int,
    val column: Int
) : SuccessfulParse<ParsedToken> {
    override val value get(): ParsedToken = this
    override val nextPosition get(): Int = tokenIndex + 1

    val text get(): String = input.substring(offset, offset + length)
}

internal class MismatchFail(expected: Token) : FailedParse("Unexpected token: '${expected.nameOrDefault}'.")

internal class EofFail(expected: Token) : FailedParse("EOF found, but '${expected.nameOrDefault}' expected.")

internal class AlternativesFail(fails: List<FailedParse>) : FailedParse(
    "Parse failures during 'or' operation:\n${fails.joinToString("\n")}"
)

internal class IncompleteParseFail(nextToken: Token) : FailedParse(
    "Cannot parse to the end: after finishing there is a token '${nextToken.nameOrDefault}'."
)

private val Token.nameOrDefault get(): String = name ?: "???"
