package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.QueryAttribute
import ru.hse.dynamomock.parser.grammars.GrammarTest

internal class ProjectionExpressionGrammarTest : GrammarTest<List<QueryAttribute>>() {
    override val grammar = ProjectionExpressionGrammar(
        mapOf("#kek" to "another_kek", "#what" to "WhAt.that---")
    )

    override fun successSource() = listOf(
        "one.two.three, \n\n\t one \t\t,\t another[0].kek[1][4]" resulted listOf(
            mv(v("one"), mv(v("two"), v("three"))), v("one"), mv(lv(v("another"), 0), lv(lv(v("kek"), 1), 4))
        ),
        "#kek[1][0].#kek.#what[123],o1n,kek,kek3,kek----" resulted listOf(
            mv(lv(lv(v("another_kek"), 1), 0), mv(v("another_kek"), lv(v("WhAt.that---"), 123))),
            v("o1n"), v("kek"), v("kek3"), v("kek----")
        )
    )

    override fun failSource() = listOf(
        failed("what,"), failed("#kek2,#kek,a"), failed("a,b,c["), failed(",ab,cd"), failed("a, \t, b")
    )
}
