package ru.hse.dynamomock.parser.grammars

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.hse.dynamomock.parser.Grammar

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal abstract class GrammarTest<T> {
    protected abstract val grammar: Grammar<T>

    abstract fun successSource(): List<Arguments>

    abstract fun failSource(): List<Arguments>

    @ParameterizedTest
    @MethodSource("successSource")
    fun testSuccessful(expression: String, expected: T) {
        assertEquals(expected, grammar.parse(expression))
    }

    @ParameterizedTest
    @MethodSource("failSource")
    fun testFailed(expression: String) {
        assertThrows<Exception> {
            grammar.parse(expression)
        }
    }

    protected infix fun String.resulted(other: T): Arguments = Arguments.of(this, other)

    protected fun failed(expression: String): Arguments = Arguments.of(expression)
}
