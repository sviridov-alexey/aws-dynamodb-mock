package ru.hse.dynamomock.model.query.grammar

import ru.hse.dynamomock.model.query.ConditionExpression
import ru.hse.dynamomock.model.query.ConditionExpression.*
import ru.hse.dynamomock.model.query.QueryAttribute
import ru.hse.dynamomock.parser.grammars.GrammarTest
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal class FilterExpressionGrammarTest : GrammarTest<ConditionExpression>() {
    override val grammar = FilterExpressionGrammar(
        mapOf("#kek" to "kEk.after", "#1one" to "one_star", "#hello" to "bye"),
        mapOf(
            ":val" to atS("value"),
            ":one" to atN("1"),
            ":age" to atN("20"),
            ":1one" to atSS("1", "one"),
            ":what" to atS("w.."),
            ":typ" to atS("SS")
        )
    )

    private fun pv(attr: AttributeValue) = Parameter.Value(attr)
    private fun pa(qAttr: QueryAttribute) = Parameter.Attribute(qAttr)
    private fun ps(qAttr: QueryAttribute) = Parameter.AttributeSize(qAttr)

    override fun successSource() = listOf(
        "#1one = :val" resulted Eq(pa(v("one_star")), pv(atS("value"))),

        "kuk < :age and other >= :what or :one <= :val AND hello > :age and hello <> other oR :age = :age" resulted Or(
            Or(
                And(Lt(pa(v("kuk")), pv(atN("20"))), Ge(pa(v("other")), pv(atS("w..")))),
                And(
                    And(Le(pv(atN("1")), pv(atS("value"))), Gt(pa(v("hello")), pv(atN("20")))),
                    Neq(pa(v("hello")), pa(v("other")))
                )
            ),
            Eq(pv(atN("20")), pv(atN("20")))
        ),

        "sa22VE = :val and not (save <> :val) or not NOT not (not a = a)" resulted Or(
            And(Eq(pa(v("sa22VE")), pv(atS("value"))), Not(Neq(pa(v("save")), pv(atS("value"))))),
            Not(Not(Not(Not(Eq(pa(v("a")), pa(v("a")))))))
        ),

        "((a) = (b)) and ((a) = (c) OR c < (:val))   " resulted And(
            Eq(pa(v("a")), pa(v("b"))), Or(Eq(pa(v("a")), pa(v("c"))), Lt(pa(v("c")), pv(atS("value"))))
        ),

        "one.two[12].second = size \n\n\n (hello)" resulted Eq(
            pa(mv(v("one"), mv(lv(v("two"), 12), v("second")))),
            ps(v("hello"))
        ),

        "one = :age and one bEtwEEn second and :age and #kek > :age" resulted And(
            And(
                Eq(pa(v("one")), pv(atN("20"))),
                Between(pa(v("one")), pa(v("second")), pv(atN("20")))
            ),
            Gt(pa(v("kEk.after")), pv(atN("20")))
        ),

        "size (value) iN  \t \t \t (one, :val, :1one) AND #hello.other = :val" resulted And(
            In(ps(v("value")), listOf(pa(v("one")), pv(atS("value")), pv(atSS("1", "one")))),
            Eq(pa(mv(v("bye"), v("other"))), pv(atS("value")))
        ),

        "attribute_exists (value) AND attribute_type(value, :typ) OR not attribute_not_exists (value)" resulted Or(
            And(AttributeExists(pa(v("value"))), AttributeType(pa(v("value")), pv(atS("SS")))),
            Not(Not(AttributeExists(pa(v("value")))))
        ),

        "not begins_with (a, b) and begins_with (a, :one) or not contains (two, :val) or contains (a, b)" resulted Or(
            Or(
                And(Not(BeginsWith(pa(v("a")), pa(v("b")))), BeginsWith(pa(v("a")), pv(atN("1")))),
                Not(Contains(pa(v("two")), pv(atS("value"))))
            ),
            Contains(pa(v("a")), pa(v("b")))
        ),

        "not not begins_with(#kek, :val) and one between :val and :what and ((a1) <> :val or " +
                "not size (a) < size (b)) or not (not not not c23c.kek <= b23b and c in ((:val), #kek))" resulted Or(
            And(
                And(
                    Not(Not(BeginsWith(pa(v("kEk.after")), pv(atS("value"))))),
                    Between(pa(v("one")), pv(atS("value")), pv(atS("w..")))
                ),
                Or(Neq(pa(v("a1")), pv(atS("value"))), Not(Lt(ps(v("a")), ps(v("b")))))
            ),
            Not(And(
                Not(Not(Not(Le(pa(mv(v("c23c"), v("kek"))), pa(v("b23b")))))),
                In(pa(v("c")), listOf(pv(atS("value")), pa(v("kEk.after"))))
            ))
        )
    )

    // TODO add tests when operands are equal (it's forbidden by DynamoDB)
    // TODO commented expressions should fail but they work
    override fun failSource() = listOf(
        failed("one = "), /*failed("((a = b))"), failed("((a)) = b"), failed("a = b and ((a = b))"),*/ failed("a = 3"),
        failed("a <=> b"), failed("< one"), failed("a[0 = b"), failed("#exists = b"), failed("a = :exists"),
        failed("function (a, b)"), failed("begins_with (a a)"), failed("BEGINS_with(a, b)"),
        failed("size (:val) = :age"), failed("contains(:val, :val)"), failed("a = :val and"),
        failed ("a = b not or b = a")
    )
}
