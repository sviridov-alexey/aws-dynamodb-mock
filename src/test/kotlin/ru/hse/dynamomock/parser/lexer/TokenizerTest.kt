package ru.hse.dynamomock.parser.lexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

internal class TokenizerTest {
    @ParameterizedTest
    @MethodSource("tokenizerTestSource")
    fun `test tokenizer`(tokens: List<Token>, input: CharSequence, expectedResult: List<Token>) {
        assertEquals(expectedResult, DefaultTokenizer(tokens).tokenize(input).map { it.token }.toList())
    }

    @Test
    fun `test tokenizer unknown token`() {
        assertThrows<IllegalArgumentException> {
            DefaultTokenizer(listOf(LiteralToken("a"), LiteralToken("b"))).tokenize("abc").toList()
        }
        assertThrows<IllegalArgumentException> {
            DefaultTokenizer(listOf(RegexToken("[a-z]+"))).tokenize("abcDefghijk").toList()
        }
    }

    companion object {
        private val name = LiteralToken("name")
        private val age = LiteralToken("age")
        private val word = RegexToken("[a-zA-Z]+")
        private val number = RegexToken("\\d+")
        private val ws = RegexToken("\\s+", ignore = true)
        private val equals = LiteralToken("=", ignore = true)
        private val comma = LiteralToken(",", ignore = true)

        @JvmStatic
        fun tokenizerTestSource() = listOf(
            Arguments.of(listOf(name, word, ws, equals), "name = Kirill", listOf(name, word)),
            Arguments.of(
                listOf(name, age, word, number, ws, equals),
                "name = Kirill age = 20",
                listOf(name, word, age, number)
            ),
            Arguments.of(listOf(word, comma, ws), "one, two,  three , \n\n word,  , ", listOf(word, word, word, word)),
            Arguments.of(
                listOf(number, word, equals, comma, ws),
                "name = 15, age = 20, skill =100500, howDidYouDoThat=2",
                listOf(word, number, word, number, word, number, word, number)
            ),
            Arguments.of(listOf(number, equals), "15=13=11=9=7=5=3=1", List(8) { number }),
            Arguments.of(listOf(name, word), "nameOther", listOf(word))
        )
    }
}
