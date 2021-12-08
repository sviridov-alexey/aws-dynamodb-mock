package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.*

internal class OrCombinator<T>(
    private val first: OrdinaryParser<T>,
    private val second: OrdinaryParser<T>
) : OrdinaryParser<T> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<T> {
        val fails = mutableListOf<FailedParse>()
        for (parser in listOf(first, second)) {
            when (val result = parser.parse(tokens, fromIndex)) {
                is SuccessfulParse -> return result
                is FailedParse -> fails += result
            }
        }
        return AlternativesFail(fails)
    }
}

internal infix fun <T> OrdinaryParser<T>.or(other: OrdinaryParser<T>): OrdinaryParser<T> = OrCombinator(this, other)
