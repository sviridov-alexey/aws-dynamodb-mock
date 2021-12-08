package ru.hse.dynamomock.parser.grammars

import ru.hse.dynamomock.parser.Grammar
import ru.hse.dynamomock.parser.OrdinaryParser
import ru.hse.dynamomock.parser.combinators.*
import ru.hse.dynamomock.parser.grammars.MathExpression.*
import ru.hse.dynamomock.parser.lexer.LiteralToken
import ru.hse.dynamomock.parser.lexer.RegexToken

internal sealed interface MathExpression {
    data class Value(val value: Int) : MathExpression
    data class Variable(val name: String) : MathExpression
    data class UnaryMinus(val expression: MathExpression) : MathExpression
    data class Sum(val left: MathExpression, val right: MathExpression) : MathExpression
    data class Subtract(val left: MathExpression, val right: MathExpression) : MathExpression
    data class Product(val left: MathExpression, val right: MathExpression) : MathExpression
    data class Divide(val left: MathExpression, val right: MathExpression) : MathExpression
    data class Power(val base: MathExpression, val exponent: MathExpression) : MathExpression
}

internal object MathExpressionGrammar : Grammar<MathExpression>() {
    private val number by RegexToken("\\d+")
    private val variable by RegexToken("[a-zA-Z][a-zA-Z0-9]*")
    private val cross by LiteralToken("*")
    private val plus by LiteralToken("+")
    private val minus by LiteralToken("-")
    private val power by LiteralToken("^")
    private val divide by LiteralToken("/")
    private val lp by LiteralToken("(")
    private val rp by LiteralToken(")")
    private val ws by RegexToken("\\s+", ignore = true)

    private val primary: OrdinaryParser<MathExpression> by
            (number map { Value(it.text.toInt()) }) or
            (variable map { Variable(it.text) }) or
            (-lp * ref(this::parser) * -rp) or
            (-minus * ref(this::parser) map { UnaryMinus(it) })

    private val powers by rightAssociated(primary, power) { l, _, r -> Power(l, r) }

    private val products by leftAssociated(powers, cross or divide) { l, r, op ->
        if (op.token === cross) Product(l, r) else Divide(l, r)
    }

    private val sums by leftAssociated(products, plus or minus) { l, r, op ->
        if (op.token === plus) Sum(l, r) else Subtract(l, r)
    }

    override val parser get() = sums
}

internal class MathExpressionGrammarTest : GrammarTest<MathExpression>() {
    override val grammar get() = MathExpressionGrammar

    private fun v(value: Int) = Value(value)
    private fun v(name: String) = Variable(name)

    private fun sum(left: MathExpression, right: MathExpression) = Sum(left, right)
    private fun sub(left: MathExpression, right: MathExpression) = Subtract(left, right)
    private fun prod(left: MathExpression, right: MathExpression) = Product(left, right)
    private fun div(left: MathExpression, right: MathExpression) = Divide(left, right)
    private fun pow(base: MathExpression, exponent: MathExpression) = Power(base, exponent)
    private fun um(expression: MathExpression) = UnaryMinus(expression)

    override fun successSource() = listOf(
        "1 + 2 +   \n 3" resulted Sum(Sum(v(1), v(2)), v(3)),
        "(1 + 2) * 3 / 2 * (-(a - b))" resulted
                prod(div(prod(sum(v(1), v(2)), v(3)), v(2)), um(sub(v("a"), v("b")))),
        "---1" resulted um(um(um(v(1)))),
        "exp / (1 + 432)^(a - 2)^(exp * 2) * 2 - 2 * abs" resulted sub(
            prod(div(v("exp"), pow(sum(v(1), v(432)), pow(sub(v("a"), v(2)), prod(v("exp"), v(2))))), v(2)),
            prod(v(2), v("abs"))
        ),
        "first * second - second  ^ \t\t exp ^ 2 * 2 - 3" resulted
                sub(sub(prod(v("first"), v("second")), prod(pow(v("second"), pow(v("exp"), v(2))), v(2))), v(3)),
        "e12 + e23fs" resulted sum(v("e12"), v("e23fs")),
        "a - -2" resulted sub(v("a"), um(v(2)))
    )

    override fun failSource() = listOf(
        failed("1 + 2 +"), failed("1^2^3^"), failed("(1 + 3 - 4 + 2 * (1 - 2)"), failed("2.4 - 2"),
        failed("+1"), failed("1 ^^ 2"), failed("1 ** a"), failed("^ 2"), failed("1ab + 2")
    )
}
