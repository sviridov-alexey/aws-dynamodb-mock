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
        for (token in tokensAlphabet) {
            val (offset, row, column) = state
            val matchedLength = token.match(input, offset)
            if (matchedLength == 0) {
                continue
            }

            for (it in offset until offset + matchedLength) {
                state.process(input[it])
            }

            val result = ParsedToken(token, state.tokenIndex, input, offset, matchedLength, row, column)
            state.tokenIndex += if (token.ignore) 0 else 1
            return result
        }
        return null
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
