package ru.hse.dynamomock.parser

import ru.hse.dynamomock.parser.lexer.DefaultTokenizer
import ru.hse.dynamomock.parser.lexer.Token
import kotlin.reflect.KProperty

internal abstract class Grammar<T> {
    abstract val parser: Parser<T>

    private val tokensAlphabet = mutableListOf<Token>()

    fun parse(input: CharSequence): T {
        println(tokensAlphabet.map { it.name })
        val tokens = DefaultTokenizer(tokensAlphabet).tokenize(input).toList()
        return when (val result = parser.parseToEnd(tokens, 0)) {
            is SuccessfulParse -> result.value
            is FailedParse -> throw IllegalArgumentException(result.message)
        }
    }

    protected operator fun <T : Token> T.provideDelegate(thisRef: Grammar<*>, property: KProperty<*>): T = apply {
        tokensAlphabet += this
    }

    protected operator fun <T : Token> T.getValue(thisRef: Grammar<*>, property: KProperty<*>): T = this

    protected operator fun <T : OrdinaryParser<*>> T.getValue(thisRef: Grammar<*>, property: KProperty<*>): T = this
}
