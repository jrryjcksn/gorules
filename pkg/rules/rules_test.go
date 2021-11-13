package rules

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Simple Expression Tests", func() {
	emptyRefFunc := func() map[string]bool {
		return emptyRefs
	}

	var args InstantiationArgs
	var count int

	BeforeEach(func() {
		count = 0
		args = InstantiationArgs{
			Name: "obj",
			Tables: map[string]string{
				"obj":              "objtab",
				"otherObject":      "otherObjTab",
				"yetAnotherObject": "yetAnotherObjTab",
			},
			gensymCount: &count,
		}
	})

	DescribeTable("Rule Expression Numeric Value Tests", func(nve NumericValueExp, inst func() string, refs map[string]bool) {
		results, err := nve.NumericGenerate().Instantiate(args)
		Expect(err).To(BeNil())
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
		results, err := cve.ComparableGenerate().Instantiate(args)
		Expect(err).To(BeNil())
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

	DescribeTable("Rule Expression Iterated Value Tests", func(ive IterableValueExp, inst func() string, refs map[string]bool, tables map[string]string) {
		results, err := ive.IterableValueGenerate().Instantiate(args)
		Expect(err).To(BeNil())
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs))
		Expect(results.Tables).To(Equal(tables))
	},
		Entry(
			"Test json array constant",
			Array(String("foo"), Number(6), Bool(true)),
			func() string { return `select value from json_each('["foo",6,true]')` },
			emptyRefs,
			emptyTables),
		Entry(
			"Test Iterable Field",
			Field("foo", "bar"),
			func() string {
				return `select each1.value from objtab objtab0, json_each(objtab0.data, '$.foo.bar') each1 where objtab0.id = objtab.id`
			},
			emptyRefs,
			emptyTables),
		Entry(
			"Test Iterable Join Field",
			JoinField("otherObject", "foo", "bar"),
			func() string {
				return `select each1.value from otherObjTab otherObjTab0, json_each(otherObjTab0.data, '$.foo.bar') each1 where otherObjTab0.id = otherObjTab.id`
			},
			emptyRefs,
			emptyTables))

	DescribeTable("Rule Expression Iterated Key Tests", func(ike IterableKeyExp, inst func() string, refs map[string]bool, tables map[string]string) {
		results, err := ike.IterableKeyGenerate().Instantiate(args)
		Expect(err).To(BeNil())
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs))
		Expect(results.Tables).To(Equal(tables))
	},
		Entry(
			"Test json array constant",
			Array(String("foo"), Number(6), Bool(true)),
			func() string { return `select key from json_each('["foo",6,true]')` },
			emptyRefs,
			emptyTables),
		Entry(
			"Test Iterable Field",
			Field("foo", "bar"),
			func() string {
				return `select each1.key from objtab objtab0, json_each(objtab0.data, '$.foo.bar') each1 where objtab0.id = objtab.id`
			},
			emptyRefs,
			emptyTables),
		Entry(
			"Test Iterable Join Field",
			JoinField("otherObject", "foo", "bar"),
			func() string {
				return `select each1.key from otherObjTab otherObjTab0, json_each(otherObjTab0.data, '$.foo.bar') each1 where otherObjTab0.id = otherObjTab.id`
			},
			emptyRefs,
			emptyTables))

	DescribeTable("Rule Expression Iterated Object Tests", func(ioe IterableObjectExp, inst func() string, refs map[string]bool, tables map[string]string) {
		results, err := ioe.IterableObjectGenerate().Instantiate(args)
		Expect(err).To(BeNil())
		Expect(results.Exp).To(Equal(inst()))
		Expect(results.Refs).To(Equal(refs))
		Expect(results.Tables).To(Equal(tables))
	},
		Entry(
			"Test json object constant",
			Object(Attribute("foo", Number(4)), Attribute("bar", String("yow")), Attribute("baz", Bool(true))),
			func() string {
				return `select json_object(key, value) from json_each('{"bar":"yow","baz":true,"foo":4}')`
			},
			emptyRefs,
			emptyTables),
		Entry(
			"Test Iterable Field",
			Field("foo", "bar"),
			func() string {
				return `select json_object(each1.key, each1.value) from objtab objtab0, json_each(objtab0.data, '$.foo.bar') each1 where objtab0.id = objtab.id`
			},
			emptyRefs,
			emptyTables),
		Entry(
			"Test Iterable Join Field",
			JoinField("otherObject", "foo", "bar"),
			func() string {
				return `select json_object(each1.key, each1.value) from otherObjTab otherObjTab0, json_each(otherObjTab0.data, '$.foo.bar') each1 where otherObjTab0.id = otherObjTab.id`
			},
			emptyRefs,
			emptyTables))

	DescribeTable("Rule Expression Comparison Tests", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		results, err := testExp.TestGenerate().Instantiate(args)
		Expect(err).To(BeNil())
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
	DescribeTable("AND, OR and NOT", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		results, err := testExp.TestGenerate().Instantiate(args)
		Expect(err).To(BeNil())
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
			func() map[string]bool { return map[string]bool{"yetAnotherObject": true, "otherObject": true} }),
		Entry(
			"Test NOT",
			NOT(OR(LT(Number(6), JoinField("yetAnotherObject", "foop", "barp")), GT(Number(6), JoinField("otherObject", "foo", "bar")))),
			func() string {
				return "NOT((6 < json_extract(yetAnotherObject.data, '$.foop.barp')) OR (6 > json_extract(otherObject.data, '$.foo.bar')))"
			},
			func() map[string]bool { return map[string]bool{"yetAnotherObject": true, "otherObject": true} }))
})

// var _ = Describe("Rule Tuple Tests", func() {
//  var e *Engine

//  BeforeEach(func() {
//      e = NewEngine()
//  })

//  It("Generates a new tuple indexed set of instantiations", func() {
//      e.NewRule("DeploymentReplicas")
//      Expect(e.RuleIndices).To(HaveLen(1))
//      Expect(e.TupleInstantiations).To(HaveLen(1))
//      Expect(e.TupleInstantiations[0]).To(Not(BeNil()))
//  })
// })

var _ = Describe("Instantiation Tests", func() {
	var im *InstantiationManager

	BeforeEach(func() {
		im = NewInstantiationManager()
	})

	It("Adds and removes instantiations", func() {
		p0 := InstantiationPriority(0)
		i1 := &Instantiation{}
		im.AddInstantiation(p0, i1)
		Expect(im.Priorities[p0].Populated).To(HaveLen(1))
		Expect(im.Priorities[p0].Free).To(HaveLen(0))
		i2 := &Instantiation{}
		im.AddInstantiation(p0, i2)
		Expect(im.Priorities[p0].Populated).To(HaveLen(2))
		Expect(im.Priorities[p0].Free).To(HaveLen(0))
		im.RemoveInstantiation(i1)
		Expect(im.Priorities[p0].Populated).To(HaveLen(2))
		Expect(im.Priorities[p0].Free).To(HaveLen(1))
		i3 := &Instantiation{}
		im.AddInstantiation(p0, i3)
		Expect(im.Priorities[p0].Populated).To(HaveLen(2))
		Expect(im.Priorities[p0].Free).To(HaveLen(0))
		i4 := &Instantiation{}
		im.AddInstantiation(p0, i4)
		Expect(im.Priorities[p0].Populated).To(HaveLen(3))
		Expect(im.Priorities[p0].Free).To(HaveLen(0))

		p10 := InstantiationPriority(10)
		Expect(im.Priorities[p10]).To(BeNil())

		i10_1 := &Instantiation{}
		im.AddInstantiation(p10, i10_1)
		Expect(im.Priorities[p10].Populated).To(HaveLen(1))
		Expect(im.Priorities[p10].Free).To(HaveLen(0))
		i10_2 := &Instantiation{}
		im.AddInstantiation(p10, i10_2)
		Expect(im.Priorities[p10].Populated).To(HaveLen(2))
		Expect(im.Priorities[p10].Free).To(HaveLen(0))
		im.RemoveInstantiation(i10_1)
		Expect(im.Priorities[p10].Populated).To(HaveLen(2))
		Expect(im.Priorities[p10].Free).To(HaveLen(1))
		i10_3 := &Instantiation{}
		im.AddInstantiation(p10, i10_3)
		Expect(im.Priorities[p10].Populated).To(HaveLen(2))
		Expect(im.Priorities[p10].Free).To(HaveLen(0))
		i10_4 := &Instantiation{}
		im.AddInstantiation(p10, i10_4)
		Expect(im.Priorities[p10].Populated).To(HaveLen(3))
		Expect(im.Priorities[p10].Free).To(HaveLen(0))
	})
})

var _ = Describe("Matcher Tests", func() {
	var args InstantiationArgs
	var count int
	emptyRefs := map[string]bool{}

	BeforeEach(func() {
		count = 0
		args = InstantiationArgs{
			Name:      "foo",
			RuleIndex: 0,
			KindsToTables: map[string]string{
				"*": "Resources",
			},
			Tables: map[string]string{
				"foo": "Resources",
			},
			gensymCount: &count,
		}
	})
	DescribeTable("Simple Match Tests", func(me MatchExp, inst string, tables map[string]string, refs map[string]bool) {
		results, err := me.MatchGenerate().Instantiate(args)
		Expect(err).To(BeNil())
		Expect(results.Exp).To(Equal(inst))
		Expect(results.Tables).To(Equal(tables))
	},
		Entry(
			"One table",
			Match("Deployment", "foo", Namespace("wego-system"), LT(Field("spec", "replicas"), Number(2))),
			"(foo.Namespace = 'wego-system') AND (json_extract(foo.data, '$.spec.replicas') < 2)",
			map[string]string{"foo": "Resources"},
			emptyRefs,
		))
	DescribeTable("Conditions Tests", func(ce ConditionsExp, inst string, tables map[string]string, refs map[string]bool) {
		results, err := ce.ConditionsGenerate().Instantiate(args)
		Expect(err).To(BeNil())
		Expect(results.Exp).To(Equal(inst))
		Expect(results.Tables).To(Equal(tables))
	},
		Entry(
			"Two tables",
			Conditions(
				Match("Deployment", "foo", Namespace("wego-system"), LT(Field("spec", "replicas"), Number(2))),
				Match("Deployment", "bar", Namespace("wego-system"), EQ(Field("spec", "replicas"), JoinField("foo", "spec", "replicas")))),
			`SELECT json_array(0, json_array(foo.ID, bar.ID, json("[]"))) FROM Resources foo JOIN Resources bar ON foo.Kind = 'Deployment' AND bar.Kind = 'Deployment' AND ((foo.Namespace = 'wego-system') AND (json_extract(foo.data, '$.spec.replicas') < 2)) AND ((bar.Namespace = 'wego-system') AND (json_extract(bar.data, '$.spec.replicas') = json_extract(foo.data, '$.spec.replicas')))`,
			map[string]string{"foo": "Resources", "bar": "Resources"},
			emptyRefs,
		))
})
