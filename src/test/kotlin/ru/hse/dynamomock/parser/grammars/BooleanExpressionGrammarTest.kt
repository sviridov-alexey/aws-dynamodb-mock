package ru.hse.dynamomock.parser.grammars

import ru.hse.dynamomock.parser.Grammar
import ru.hse.dynamomock.parser.OrdinaryParser
import ru.hse.dynamomock.parser.combinators.*
import ru.hse.dynamomock.parser.grammars.BooleanExpression.*
import ru.hse.dynamomock.parser.grammars.BooleanExpression.Literal.False
import ru.hse.dynamomock.parser.grammars.BooleanExpression.Literal.True
import ru.hse.dynamomock.parser.lexer.LiteralToken
import ru.hse.dynamomock.parser.lexer.RegexToken

internal sealed interface BooleanExpression {
    data class And(val left: BooleanExpression, val right: BooleanExpression) : BooleanExpression
    data class Or(val left: BooleanExpression, val right: BooleanExpression) : BooleanExpression
    data class Not(val expression: BooleanExpression) : BooleanExpression
    data class Variable(val name: String) : BooleanExpression

    interface Literal : BooleanExpression {
        object True : Literal {
            override fun toString(): String = "True"
        }
        object False : Literal {
            override fun toString(): String = "False"
        }
    }
}

internal object BooleanExpressionGrammar : Grammar<BooleanExpression>() {
    private val andT by LiteralToken("&")
    private val orT by LiteralToken("|")
    private val notT by LiteralToken("!")
    private val trueT by LiteralToken("True")
    private val falseT by LiteralToken("False")
    private val variableT by RegexToken("[a-zA-Z0-9]+")
    private val rp by LiteralToken(")")
    private val lp by LiteralToken("(")
    @Suppress("unused")
    private val ws by RegexToken("\\s+", ignore = true)

    private val term: OrdinaryParser<BooleanExpression> by
        (variableT map { Variable(it.text) }) or
        (-notT * ref(this::term) map { Not(it) }) or
        (-lp * ref(this::parser) * -rp) or
        (trueT map { True }) or
        (falseT map { False })

    private val ands by leftAssociated(term, andT) { l, r -> And(l, r) }
    override val parser by leftAssociated(ands, orT) { l, r -> Or(l, r) }
}

internal class BooleanExpressionGrammarTest : GrammarTest<BooleanExpression>() {
    private fun v(name: String) = Variable(name)

    override val grammar = BooleanExpressionGrammar

    override fun successSource() = listOf(
        "a & b" resulted And(v("a"), v("b")),
        "!a | (((a & c)))" resulted Or(Not(v("a")), And(v("a"), v("c"))),
        "True & False | True & False & !(a & b | c) | a" resulted
                Or(Or(And(True, False), And(And(True, False), Not(Or(And(v("a"), v("b")), v("c"))))), v("a")),
        "FiRs1 & sEc11Ond2S | True3" resulted Or(And(v("FiRs1"), v("sEc11Ond2S")), v("True3")),
        "!!a | b" resulted Or(Not(Not(v("a"))), v("b"))
    )

    override fun failSource() = listOf(
        failed("a &"), failed("& b"), failed("!"), failed("!a | (a & b"), failed("a | | b"),
        failed("a | b)"), failed("True | False & a & !")
    )
}
