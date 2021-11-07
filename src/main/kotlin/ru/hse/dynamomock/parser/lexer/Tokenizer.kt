package ru.hse.dynamomock.parser.lexer

import ru.hse.dynamomock.parser.ParsedToken

internal interface Tokenizer {
    fun tokenize(input: CharSequence): Sequence<ParsedToken>
}

internal class DefaultTokenizer(private val tokensAlphabet: List<Token>) : Tokenizer {
    override fun tokenize(input: CharSequence): Sequence<ParsedToken> {
        val state = State()
        return generateSequence { nextNotIgnoredToken(state, input) }
    }

    private fun nextNotIgnoredToken(state: State, input: CharSequence): ParsedToken? {
        while (true) {
            val next = nextToken(state, input)
            if (next == null) {
                require(input.length == state.offset) {
                    "Cannot tokenize the whole input. Unknown token: row=${state.row}, column=${state.column}."
                }
                return null
            } else if (!next.token.ignore) {
                return next
            }
        }
    }

    private fun nextToken(state: State, input: CharSequence): ParsedToken? {
        if (state.offset >= input.length) {
            return null
        }

        val matched = tokensAlphabet.mapNotNull { token ->
            val matchedLength = token.match(input, state.offset)
            if (matchedLength > 0) matchedLength to token else null
        }
        return matched.maxByOrNull { it.first }?.let { (matchedLength, token) ->
            val (offset, row, column) = state
            for (it in offset until offset + matchedLength) {
                state.process(input[it])
            }

            val result = ParsedToken(token, state.tokenIndex, input, offset, matchedLength, row, column)
            if (!token.ignore) {
                state.tokenIndex++
            }
            result
        }
    }

    private data class State(var offset: Int = 0, var row: Int = 1, var column: Int = 1, var tokenIndex: Int = 0) {
        fun process(symbol: Char) {
            if (symbol == '\n') {
                row++
                column = 0
            }
            column++
            offset++
        }
    }
}
