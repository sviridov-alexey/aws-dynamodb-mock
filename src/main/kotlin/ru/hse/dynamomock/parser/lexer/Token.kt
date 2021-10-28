package ru.hse.dynamomock.parser.lexer

import ru.hse.dynamomock.parser.*

internal abstract class Token(var name: String?, val ignore: Boolean) : OrdinaryParser<ParsedToken> {
    abstract fun match(input: CharSequence, fromIndex: Int): Int

    override fun parse(tokens: List<ParsedToken>, fromIndex: Int): ParseResult<ParsedToken> {
        val parsedToken = tokens.getOrNull(fromIndex)
        return when {
            parsedToken == null -> EofFail(this)
            parsedToken.token === this -> parsedToken
            else -> MismatchFail(this)
        }
    }
}

internal class LiteralToken(
    private val text: CharSequence,
    name: String? = null,
    ignore: Boolean = false
) : Token(name, ignore) {
    override fun match(input: CharSequence, fromIndex: Int): Int =
        if (input.startsWith(text, fromIndex)) text.length else 0
}

internal class RegexToken(
    regex: String,
    options: Set<RegexOption> = emptySet(),
    name: String? = null,
    ignore: Boolean = false
) : Token(name, ignore) {
    private val matcher = regex.toRegex(options).toPattern().matcher("")

    override fun match(input: CharSequence, fromIndex: Int): Int {
        matcher.reset(input).region(fromIndex, input.length)
        if (!matcher.find() || matcher.start() != fromIndex) {
            return 0
        }
        return matcher.end() - fromIndex
    }
}
