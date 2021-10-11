package rules

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Context("Simple Expression Tests", func() {
	var args InstantiationArgs

	BeforeEach(func() {
		gensymCount := 0
		args = InstantiationArgs{
			Name:        "obj",
			gensymCount: &gensymCount,
		}
	})

	DescribeTable("Rule Expression Numeric Value Tests", func(nve NumericValueExp, inst func() string, refs map[string]bool) {
		instantiated, objectReferences := nve.NumericGenerate().Instantiate(args)
		Expect(instantiated).To(Equal(inst()))
		Expect(objectReferences).To(Equal(refs))
	},
		Entry(
			"Test simple number",
			Number(6),
			func() string { return "6" },
			map[string]bool{}),
		Entry(
			"Test simple field",
			Field("foo", "bar"),
			func() string { return fmt.Sprintf("json_extract(%s.data, '$.foo.bar')", args.Name) },
			map[string]bool{}),
		Entry(
			"Test join field",
			JoinField("otherObject", "foo", "bar"),
			func() string { return "json_extract(otherObject.data, '$.foo.bar')" },
			map[string]bool{"otherObject": true}))

	DescribeTable("Rule Expression Comparable Value Tests", func(cve ComparableValueExp, inst func() string, refs map[string]bool) {
		instantiated, objectReferences := cve.ComparableGenerate().Instantiate(args)
		Expect(instantiated).To(Equal(inst()))
		Expect(objectReferences).To(Equal(refs))
	},
		Entry(
			"Test simple number",
			Number(6),
			func() string { return "6" },
			map[string]bool{}),
		Entry(
			"Test simple field",
			Field("foo", "bar"),
			func() string { return fmt.Sprintf("json_extract(%s.data, '$.foo.bar')", args.Name) },
			map[string]bool{}),
		Entry(
			"Test join field",
			JoinField("otherObject", "foo", "bar"),
			func() string { return "json_extract(otherObject.data, '$.foo.bar')" },
			map[string]bool{"otherObject": true}),
		Entry(
			"Test string constant",
			String("a string"),
			func() string { return "'a string'" },
			map[string]bool{}))

	DescribeTable("Rule Expression Comparison Tests", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		instantiated, objectReferences := testExp.TestGenerate().Instantiate(args)
		Expect(instantiated).To(Equal(inst()))
		Expect(objectReferences).To(Equal(refs()))
	},
		Entry(
			"Test less than",
			LT(Number(6), Field("foo", "bar")),
			func() string { return fmt.Sprintf("6 < json_extract(%s.data, '$.foo.bar')", args.Name) },
			func() map[string]bool { return map[string]bool{} }),
		Entry(
			"Test less than or equal",
			LE(Number(6), Field("foo", "bar")),
			func() string { return fmt.Sprintf("6 <= json_extract(%s.data, '$.foo.bar')", args.Name) },
			func() map[string]bool { return map[string]bool{} }),
		Entry(
			"Test greater than",
			GT(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 > json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test greater than or equal",
			GE(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 >= json_extract(otherObject.data, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }))
})
