package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.QueryAttribute
import ru.hse.dynamomock.parser.grammars.GrammarTest

internal class ProjectionExpressionGrammarTest : GrammarTest<List<QueryAttribute>>() {
    override val grammar = ProjectionExpressionGrammar(
        mapOf(
            "one" to "1one", "aNo22ThEr" to "kek.Another.Kek",
            "1test" to "_.another-Test11...", "WHaT" to "w.H.A.T"
        )
    )

    private fun v(name: String) = QueryAttribute.Value(name)
    private fun mv(attribute: QueryAttribute, value: QueryAttribute) = QueryAttribute.MapValue(attribute, value)
    private fun lv(attribute: QueryAttribute, index: Int) = QueryAttribute.ListValue(attribute, index)

    override fun successSource() = listOf(
        "first.second[1]" resulted listOf(mv(v("first"), lv(v("second"), 1))),
        "f1RsT--2_" resulted listOf(v("f1RsT--2_")),
        "#one[1][2].#1test[1112][0][23].kek2" resulted listOf(mv(
            lv(lv(v("1one"), 1), 2),
            mv(lv(lv(lv(v("_.another-Test11..."), 1112), 0), 23), v("kek2"))
        )),
        "#WHaT.#WHaT.#aNo22ThEr" resulted listOf(mv(v("w.H.A.T"), mv(v("w.H.A.T"), v("kek.Another.Kek")))),
        "kek,kek,kek,kek" resulted List(4) { v("kek") },
        "#one[0].kek,a.b.c,    a,b,  C,\t\tD" resulted listOf(
            mv(lv(v("1one"), 0), v("kek")), mv(v("a"), mv(v("b"), v("c"))), v("a"), v("b"), v("C"), v("D")
        )
    )

    override fun failSource() = listOf(
        failed("a,"), failed("1a,b"), failed("#non"), failed("a-bcd"), failed("abcd[0"), failed("abcd[a]"),
        failed("abcd[#one]"), failed("a.A.A[1][2]]"), failed("ab[1,2]"), failed("[0]"), failed(",abcd")
    )
}
