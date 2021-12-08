package ru.hse.dynamomock.parser.lexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

internal class TokenTest {
    private fun Token.test(input: CharSequence, fromIndex: Int, expectedLength: Int) {
        assertEquals(expectedLength, match(input, fromIndex))
    }

    @ParameterizedTest
    @MethodSource("literalTokenTestSources")
    fun `test literal token`(literal: String, input: CharSequence, fromIndex: Int, expectedLength: Int) {
        LiteralToken(literal).test(input, fromIndex, expectedLength)
    }

    @ParameterizedTest
    @MethodSource("regexTokenTestSources")
    fun `test regex token`(
        regex: String,
        options: Set<RegexOption>,
        input: CharSequence,
        fromIndex: Int,
        expectedLength: Int
    ) {
        RegexToken(regex, options).test(input, fromIndex, expectedLength)
    }

    companion object {
        @JvmStatic
        fun literalTokenTestSources() = listOf(
            Arguments.of("friend", "Hey, my friend!", 8, 6),
            Arguments.of("friend", "Fried chicken.", 0, 0),
            Arguments.of("friend", "How dare you, my friend???", 17, 6),
            Arguments.of("one two", "One two three four five", 0, 0),
            Arguments.of("one two", "one two three four five", 0, 7),
            Arguments.of("one two", "one, two, three, four, five", 0, 0)
        )

        @JvmStatic
        fun regexTokenTestSources() = listOf(
            Arguments.of("[0-9]+", emptySet<RegexOption>(), "My number is: 4002-3", 14, 4),
            Arguments.of("[a-gA-Z]+", emptySet<RegexOption>(), "AbCdEfGHiJk", 0, 8),
            Arguments.of(".*", emptySet<RegexOption>(), "hi, my friend!##$", 0, 17),
            Arguments.of("\\s+", emptySet<RegexOption>(), "   \n\n\t\t\t a \t\t\t\n", 1, 8),
            Arguments.of("[a-z]+", setOf(RegexOption.IGNORE_CASE), "WoW 15", 0, 3)
        )
    }
}
