package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.QueryAttribute
import ru.hse.dynamomock.parser.grammars.GrammarTest

internal class QueryAttributeGrammarTest : GrammarTest<QueryAttribute>() {
    override val grammar = QueryAttributeGrammar(
        mapOf(
            "#one" to "1one", "#aNo22ThEr" to "kek.Another.Kek",
            "#1test" to "_.another-Test11...", "#WHaT" to "w.H.A.T"
        )
    )

    private fun v(name: String) = QueryAttribute.Simple.Value(name)
    private fun mv(attribute: QueryAttribute.Simple, value: QueryAttribute) = QueryAttribute.MapValue(attribute, value)
    private fun lv(attribute: QueryAttribute.Simple, index: Int) = QueryAttribute.Simple.ListValue(attribute, index)

    override fun successSource() = listOf(
        "first.second[1]" resulted mv(v("first"), lv(v("second"), 1)),
        "f1RsT--2_" resulted v("f1RsT--2_"),
        "#one[1][2].#1test[1112][0][23].kek2" resulted mv(
            lv(lv(v("1one"), 1), 2),
            mv(lv(lv(lv(v("_.another-Test11..."), 1112), 0), 23), v("kek2"))
        ),
        "#WHaT.#WHaT.#aNo22ThEr" resulted mv(v("w.H.A.T"), mv(v("w.H.A.T"), v("kek.Another.Kek")))
    )

    override fun failSource() = listOf(
        failed("a,b"), failed("1-ab"), failed("#non"), failed("a-bcd"), failed("aBcd.a."), failed("a[a]"),
        failed("abcd[0]["), failed("a[1,2]"), failed(":prop"), failed("[0]"), failed(".a"), failed("a[-1]")
    )
}
