package ru.hse.dynamomock.parser.combinators

import ru.hse.dynamomock.parser.OrdinaryParser
import ru.hse.dynamomock.parser.ParseResult
import ru.hse.dynamomock.parser.ParsedToken
import ru.hse.dynamomock.parser.ParsedValue

internal object EmptyCombinator : OrdinaryParser<Unit> {
    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<Unit> = ParsedValue(Unit, fromIndex)
}
