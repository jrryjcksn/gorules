package rules

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Context("Simple Expression Tests", func() {
	emptyRefs := map[string]bool{}
	emptyRefFunc := func() map[string]bool {
		return emptyRefs
	}

	var args InstantiationArgs

	BeforeEach(func() {
		gensymCount := 0

		args = InstantiationArgs{
			Name:        "obj",
			gensymCount: &gensymCount,
		}
	})

	DescribeTable("Rule Expression Numeric Value Tests", func(nve NumericValueExp, inst func() string, refs map[string]bool) {
		results := nve.NumericGenerate().Instantiate(args)
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs))
	},
		Entry(
			"Test simple number",
			Number(6),
			func() string { return "6" },
			emptyRefs),
		Entry(
			"Test simple field",
			Field("foo", "bar"),
			func() string { return fmt.Sprintf("json_extract(%s.data, '$.foo.bar')", args.Name) },
			emptyRefs),
		Entry(
			"Test join field",
			JoinField("otherObject", "foo", "bar"),
			func() string { return "json_extract(otherObject.data, '$.foo.bar')" },
			map[string]bool{"otherObject": true}))

	DescribeTable("Rule Expression Comparable Value Tests", func(cve ComparableValueExp, inst func() string, refs map[string]bool) {
		results := cve.ComparableGenerate().Instantiate(args)
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs))
	},
		Entry(
			"Test simple number",
			Number(6),
			func() string { return "6" },
			emptyRefs),
		Entry(
			"Test simple field",
			Field("foo", "bar"),
			func() string { return fmt.Sprintf("json_extract(%s.data, '$.foo.bar')", args.Name) },
			emptyRefs),
		Entry(
			"Test join field",
			JoinField("otherObject", "foo", "bar"),
			func() string { return "json_extract(otherObject.data, '$.foo.bar')" },
			map[string]bool{"otherObject": true}),
		Entry(
			"Test string constant",
			String("a string"),
			func() string { return "'a string'" },
			emptyRefs))

	DescribeTable("Rule Expression Comparison Tests", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		results := testExp.TestGenerate().Instantiate(args)
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs()))
	},
		Entry(
			"Test less than",
			LT(Number(6), Field("foo", "bar")),
			func() string { return fmt.Sprintf("6 < json_extract(%s.data, '$.foo.bar')", args.Name) },
			emptyRefFunc),
		Entry(
			"Test less than or equal",
			LE(Number(6), Field("foo", "bar")),
			func() string { return fmt.Sprintf("6 <= json_extract(%s.data, '$.foo.bar')", args.Name) },
			emptyRefFunc),
		Entry(
			"Test greater than",
			GT(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 > json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test greater than or equal",
			GE(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 >= json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test equal numbers",
			EQ(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 = json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test equal strings",
			EQ(String("grom"), JoinField("otherObject", "foo", "bar")),
			func() string { return "'grom' = json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test equal booleans",
			EQ(Bool(true), JoinField("otherObject", "foo", "bar")),
			func() string { return "true = json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),

		Entry(
			"Test not equal numbers",
			NEQ(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 <> json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test not equal strings",
			NEQ(String("grom"), JoinField("otherObject", "foo", "bar")),
			func() string { return "'grom' <> json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test not equal booleans",
			NEQ(Bool(true), JoinField("otherObject", "foo", "bar")),
			func() string { return "true <> json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }))
	DescribeTable("AND and OR", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		results := testExp.TestGenerate().Instantiate(args)
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs()))
	},
		Entry(
			"Test AND",
			AND(LT(Number(6), Field("foo", "bar")), GT(Number(6), JoinField("otherObject", "foo", "bar"))),
			func() string {
				return fmt.Sprintf("(6 < json_extract(%s.data, '$.foo.bar')) AND (6 > json_extract(otherObject.data, '$.foo.bar'))",
					args.Name)
			},
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test OR",
			OR(LT(Number(6), JoinField("yetAnotherObject", "foop", "barp")), GT(Number(6), JoinField("otherObject", "foo", "bar"))),
			func() string {
				return "(6 < json_extract(yetAnotherObject.data, '$.foop.barp')) OR (6 > json_extract(otherObject.data, '$.foo.bar'))"
			},
			func() map[string]bool { return map[string]bool{"yetAnotherObject": true, "otherObject": true} }))
})
