package rules

import (
	//	. "github.com/onsi/ginkgo"

	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("Rule Expression Numeric Value Tests", func(nve NumericValueExp, name, inst string, refs map[string]bool) {
	instantiated, objectReferences := nve.NumericGenerate().Instantiate(name)
	Expect(instantiated).To(Equal(inst))
	Expect(objectReferences).To(Equal(refs))
},
	Entry("Test simple number", Number(6), "objectName", "6", map[string]bool{}),
	Entry(
		"Test simple field",
		Field("foo", "bar"),
		"objectName",
		"json_extract(objectName.data, '$.foo.bar')",
		map[string]bool{}),
	Entry(
		"Test join field",
		JoinField("otherObject", "foo", "bar"),
		"objectName",
		"json_extract(otherObject.data, '$.foo.bar')",
		map[string]bool{"otherObject": true}))

var _ = DescribeTable("Rule Expression Comparisons", func(testExp TestExp, name, inst string, refs map[string]bool) {
	instantiated, objectReferences := testExp.TestGenerate().Instantiate(name)
	Expect(instantiated).To(Equal(inst))
	Expect(objectReferences).To(Equal(refs))
},
	Entry("Test less than", LT(Number(6), JoinField("obj", "foo", "bar")), "obj", "6 < json_extract(obj.data, '$.foo.bar')",
		map[string]bool{"obj": true}))
