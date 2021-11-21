package ru.hse.dynamomock.parser

import ru.hse.dynamomock.parser.lexer.DefaultTokenizer
import ru.hse.dynamomock.parser.lexer.Token
import kotlin.reflect.KProperty

internal abstract class Grammar<T> {
    abstract val parser: OrdinaryParser<T>

    private val tokensAlphabet = mutableListOf<Token>()

    fun parse(input: CharSequence): T {
        val tokens = DefaultTokenizer(tokensAlphabet).tokenize(input).toList()
        return when (val result = parser.parseToEnd(tokens, 0)) {
            is SuccessfulParse -> result.value
            is FailedParse -> throw IllegalArgumentException(result.message)
        }
    }

    protected operator fun <T : Token> T.provideDelegate(thisRef: Grammar<*>, property: KProperty<*>): T = apply {
        name = name ?: property.name
        tokensAlphabet += this
    }

    protected operator fun <T : Token> T.getValue(thisRef: Grammar<*>, property: KProperty<*>): T = this

    protected operator fun <T : OrdinaryParser<*>> T.getValue(thisRef: Grammar<*>, property: KProperty<*>): T = this
    
    protected fun externalGrammar(other: Grammar<*>) {
        tokensAlphabet += other.tokensAlphabet
    }
}
