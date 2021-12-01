package rules

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"database/sql"
)

var _ = Describe("Simple Expression Tests", func() {
	emptyRefFunc := func() map[string]bool {
		return emptyRefs
	}

	var args *InstantiationData

	BeforeEach(func() {
		args = &InstantiationData{
			Names:   []string{"obj", "otherObject", "yetAnotherObject"},
			Queries: map[string]Queries{},
			Refs:    map[string]bool{},
			//			FieldChecks: map[string]map[string]bool{},
			Tables: map[string]string{
				"obj":              "objtab",
				"otherObject":      "otherObjTab",
				"yetAnotherObject": "yetAnotherObjTab",
			},
		}
	})

	DescribeTable("Rule Expression Numeric Value Tests", func(nve NumericValueExp, inst func() string, refs map[string]bool, idx int) {
		results, err := nve.NumericGenerate().Instantiate(args, idx)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs))
	},
		Entry(
			"Test simple number",
			Number(6),
			func() string { return "6" },
			emptyRefs,
			0),
		Entry(
			"Test simple field",
			Field("foo", "bar"),
			func() string { return fmt.Sprintf("json_extract(%s.DATA, '$.foo.bar')", args.Names[0]) },
			emptyRefs,
			0),
		Entry(
			"Test join field",
			JoinField("otherObject", "foo", "bar"),
			func() string { return "json_extract(otherObject.DATA, '$.foo.bar')" },
			map[string]bool{"otherObject": true},
			1))

	DescribeTable("Rule Expression Comparable Value Tests", func(cve ComparableValueExp, inst func() string, refs map[string]bool) {
		results, err := cve.ComparableGenerate().Instantiate(args, 0)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs))
	},
		Entry(
			"Test simple number",
			Number(6),
			func() string { return "6" },
			emptyRefs),
		Entry(
			"Test simple field",
			Field("foo", "bar"),
			func() string { return fmt.Sprintf("json_extract(%s.DATA, '$.foo.bar')", args.Names[0]) },
			emptyRefs),
		Entry(
			"Test join field",
			JoinField("otherObject", "foo", "bar"),
			func() string { return "json_extract(otherObject.DATA, '$.foo.bar')" },
			map[string]bool{"otherObject": true}),
		Entry(
			"Test string constant",
			String("a string"),
			func() string { return "'a string'" },
			emptyRefs))

	DescribeTable("Rule Expression Iterated Value Tests", func(ive IterableValueExp, inst func() string, refs map[string]bool, tables map[string]string, idx int) {
		results, err := ive.IterableValueGenerate().Instantiate(args, idx)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs))
	},
		Entry(
			"Test json array constant",
			Array(String("foo"), Number(6), Bool(true)),
			func() string { return `select value from json_each('["foo",6,true]')` },
			emptyRefs,
			emptyTables,
			0),
		Entry(
			"Test Iterable Field",
			Field("foo", "bar"),
			func() string {
				return `select each1.value from objtab objtab0, json_each(objtab0.DATA, '$.foo.bar') each1 where objtab0.id = objtab.id`
			},
			emptyRefs,
			emptyTables,
			0),
		Entry(
			"Test Iterable Join Field",
			JoinField("otherObject", "foo", "bar"),
			func() string {
				return `select each1.value from otherObjTab otherObjTab0, json_each(otherObjTab0.DATA, '$.foo.bar') each1 where otherObjTab0.id = otherObjTab.id`
			},
			emptyRefs,
			emptyTables,
			1))

	DescribeTable("Rule Expression Iterated Key Tests", func(ike IterableKeyExp, inst func() string, refs map[string]bool, tables map[string]string, idx int) {
		results, err := ike.IterableKeyGenerate().Instantiate(args, idx)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs))
	},
		Entry(
			"Test json array constant",
			Array(String("foo"), Number(6), Bool(true)),
			func() string { return `select key from json_each('["foo",6,true]')` },
			emptyRefs,
			emptyTables,
			0),
		Entry(
			"Test Iterable Field",
			Field("foo", "bar"),
			func() string {
				return `select each1.key from objtab objtab0, json_each(objtab0.DATA, '$.foo.bar') each1 where objtab0.id = objtab.id`
			},
			emptyRefs,
			emptyTables,
			0),
		Entry(
			"Test Iterable Join Field",
			JoinField("otherObject", "foo", "bar"),
			func() string {
				return `select each1.key from otherObjTab otherObjTab0, json_each(otherObjTab0.DATA, '$.foo.bar') each1 where otherObjTab0.id = otherObjTab.id`
			},
			emptyRefs,
			emptyTables,
			1))

	DescribeTable("Rule Expression Iterated Object Tests", func(ioe IterableObjectExp, inst func() string, refs map[string]bool, tables map[string]string, idx int) {
		results, err := ioe.IterableObjectGenerate().Instantiate(args, idx)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs))
	},
		Entry(
			"Test json object constant",
			Object(Attribute("foo", Number(4)), Attribute("bar", String("yow")), Attribute("baz", Bool(true))),
			func() string {
				return `select json_object(key, value) from json_each('{"bar":"yow","baz":true,"foo":4}')`
			},
			emptyRefs,
			emptyTables,
			0),
		Entry(
			"Test Iterable Field",
			Field("foo", "bar"),
			func() string {
				return `select json_object(each1.key, each1.value) from objtab objtab0, json_each(objtab0.DATA, '$.foo.bar') each1 where objtab0.id = objtab.id`
			},
			emptyRefs,
			emptyTables,
			0),
		Entry(
			"Test Iterable Join Field",
			JoinField("otherObject", "foo", "bar"),
			func() string {
				return `select json_object(each1.key, each1.value) from otherObjTab otherObjTab0, json_each(otherObjTab0.DATA, '$.foo.bar') each1 where otherObjTab0.id = otherObjTab.id`
			},
			emptyRefs,
			emptyTables,
			1))

	DescribeTable("Rule Expression Comparison Tests", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		results, err := testExp.TestGenerate().Instantiate(args, 0)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs()))
	},
		Entry(
			"Test less than",
			LT(Number(6), Field("foo", "bar")),
			func() string { return fmt.Sprintf("6 < json_extract(%s.DATA, '$.foo.bar')", args.Names[0]) },
			emptyRefFunc),
		Entry(
			"Test less than or equal",
			LE(Number(6), Field("foo", "bar")),
			func() string { return fmt.Sprintf("6 <= json_extract(%s.DATA, '$.foo.bar')", args.Names[0]) },
			emptyRefFunc),
		Entry(
			"Test greater than",
			GT(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 > json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test greater than or equal",
			GE(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 >= json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test equal numbers",
			EQ(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 = json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test equal strings",
			EQ(String("grom"), JoinField("otherObject", "foo", "bar")),
			func() string { return "'grom' = json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test equal booleans",
			EQ(Bool(true), JoinField("otherObject", "foo", "bar")),
			func() string { return "true = json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),

		Entry(
			"Test not equal numbers",
			NEQ(Number(6), JoinField("otherObject", "foo", "bar")),
			func() string { return "6 <> json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test not equal strings",
			NEQ(String("grom"), JoinField("otherObject", "foo", "bar")),
			func() string { return "'grom' <> json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test not equal booleans",
			NEQ(Bool(true), JoinField("otherObject", "foo", "bar")),
			func() string { return "true <> json_extract(otherObject.DATA, '$.foo.bar')" },
			func() map[string]bool { return map[string]bool{"otherObject": true} }))
	DescribeTable("AND, OR and NOT", func(testExp TestExp, inst func() string, refs func() map[string]bool) {
		fmt.Printf("X: %#v\n", args)
		results, err := testExp.TestGenerate().Instantiate(args, 0)
		Expect(err).To(BeNil())
		Expect(results).To(Equal(inst()))
		Expect(args.Refs).To(Equal(refs()))
	},
		Entry(
			"Test AND",
			AND(LT(Number(6), Field("foo", "bar")), GT(Number(6), JoinField("otherObject", "foo", "bar"))),
			func() string {
				return fmt.Sprintf("(6 < json_extract(%s.DATA, '$.foo.bar')) AND (6 > json_extract(otherObject.DATA, '$.foo.bar'))",
					args.Names[0])
			},
			func() map[string]bool { return map[string]bool{"otherObject": true} }),
		Entry(
			"Test OR",
			OR(LT(Number(6), JoinField("yetAnotherObject", "foop", "barp")), GT(Number(6), JoinField("otherObject", "foo", "bar"))),
			func() string {
				return "(6 < json_extract(yetAnotherObject.DATA, '$.foop.barp')) OR (6 > json_extract(otherObject.DATA, '$.foo.bar'))"
			},
			func() map[string]bool { return map[string]bool{"yetAnotherObject": true, "otherObject": true} }),
		Entry(
			"Test NOT",
			NOT(OR(LT(Number(6), JoinField("yetAnotherObject", "foop", "barp")), GT(Number(6), JoinField("otherObject", "foo", "bar")))),
			func() string {
				return "NOT((6 < json_extract(yetAnotherObject.DATA, '$.foop.barp')) OR (6 > json_extract(otherObject.DATA, '$.foo.bar')))"
			},
			func() map[string]bool { return map[string]bool{"yetAnotherObject": true, "otherObject": true} }))
})

var _ = Describe("Rule Tests", func() {
	var args *InstantiationData

	BeforeEach(func() {
		args = &InstantiationData{
			Names:   []string{"foo", "bar"},
			Queries: map[string]Queries{},
			Refs:    map[string]bool{},
			//			FieldChecks: map[string]map[string]bool{},
			RuleIndex: 20,
			Priority:  10,
		}
	})

	It("generates the correct queries and indices", func() {
		_, err := Rule(
			Name("rule1"),
			Conditions(
				Match("Deployment", "foo", Namespace("wego-system"), LT(Field("spec", "replicas"), Number(2))),
				Match("Deployment", "bar", Namespace("wego-system"), GT(Field("spec", "replicas"), JoinField("foo", "spec", "replicas")))),
			Actions(
				func(args []interface{}) error {
					foo := args[0]
					bar := args[1]
					fmt.Printf("FOO: %#v\n, BAR: %#v\n", foo, bar)
					return nil
				})).Instantiate(args, 0)
		Expect(err).ShouldNot(HaveOccurred())
		//		Expect(args.FieldChecks["foo"]).Should(HaveKey("json_extract(NEW.DATA, '$.spec.replicas') <> json_extract(OLD.DATA, '$.spec.replicas')"))
		//		Expect(args.FieldChecks["bar"]).Should(HaveKey("json_extract(NEW.DATA, '$.spec.replicas') <> json_extract(OLD.DATA, '$.spec.replicas')"))
		Expect(args.Queries[""].Insert).Should(Equal(fmt.Sprintf("INSERT INTO instantiations (ruleNum, priority, resources) SELECT %d, %d, json_array(foo.ID, bar.ID) FROM resources foo, resources bar WHERE foo.KIND = 'Deployment' AND bar.KIND = 'Deployment' AND ((foo.NAMESPACE = 'wego-system') AND json_extract(foo.DATA, '$.spec.replicas') < 2) AND ((bar.NAMESPACE = 'wego-system') AND json_extract(bar.DATA, '$.spec.replicas') > json_extract(foo.DATA, '$.spec.replicas'))", args.RuleIndex, args.Priority)))
		Expect(args.Queries["foo"].Insert).Should(Equal(fmt.Sprintf("CREATE TRIGGER foo_resources_%d AFTER INSERT ON resources WHEN NEW.KIND = 'Deployment' BEGIN INSERT INTO instantiations (ruleNum, priority, resources) SELECT %d, %d, json_array(foo.ID, bar.ID) FROM resources foo, resources bar WHERE foo.KIND = 'Deployment' AND bar.KIND = 'Deployment' AND ((foo.NAMESPACE = 'wego-system') AND json_extract(foo.DATA, '$.spec.replicas') < 2) AND ((bar.NAMESPACE = 'wego-system') AND json_extract(bar.DATA, '$.spec.replicas') > json_extract(foo.DATA, '$.spec.replicas')) AND foo.ID = NEW.ID; END", args.RuleIndex, args.RuleIndex, args.Priority)))
		Expect(args.Queries["bar"].Insert).Should(Equal(fmt.Sprintf("CREATE TRIGGER bar_resources_%d AFTER INSERT ON resources WHEN NEW.KIND = 'Deployment' BEGIN INSERT INTO instantiations (ruleNum, priority, resources) SELECT %d, %d, json_array(foo.ID, bar.ID) FROM resources foo, resources bar WHERE foo.KIND = 'Deployment' AND bar.KIND = 'Deployment' AND ((foo.NAMESPACE = 'wego-system') AND json_extract(foo.DATA, '$.spec.replicas') < 2) AND ((bar.NAMESPACE = 'wego-system') AND json_extract(bar.DATA, '$.spec.replicas') > json_extract(foo.DATA, '$.spec.replicas')) AND bar.ID = NEW.ID; END", args.RuleIndex, args.RuleIndex, args.Priority)))
	})

	It("processes an instantiation result set", func() {
		e, err := NewTestEngine()
		Expect(err).ShouldNot(HaveOccurred())
		connections, err := getResourceInstantiationConnections(e.DB)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(len(connections)).To(Equal(2))
		err = e.AddResourceStringList([]string{`{"kind": "Ball", "namespace": "test", "name": "foo", "color": "red", "size": 10}`})
		Expect(err).ShouldNot(HaveOccurred())
		r, err := e.GetResource("Ball", "foo", "test")
		Expect(err).ShouldNot(HaveOccurred())
		var m map[string]interface{}
		err = json.Unmarshal([]byte(r), &m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(m["color"]).To(Equal("red"))
		connections, err = getResourceInstantiationConnections(e.DB)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(len(connections)).To(Equal(0))
		err = e.AddResourceStringList([]string{`{"kind": "Ball", "namespace": "test", "name": "foo", "color": "reddish", "size": 10}`})
		r, err = e.GetResource("Ball", "foo", "test")
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal([]byte(r), &m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(m["color"]).To(Equal("reddish"))
	})
})

// var _ = Describe("Matcher Tests", func() {
//  var args InstantiationArgs
//  var count int
//  //	emptyRefs := map[string]bool{}

//  BeforeEach(func() {
//      count = 0
//      args = InstantiationArgs{
//          Name:      "foo",
//          RuleIndex: 0,
//          Tables: map[string]string{
//              "foo": "Resources",
//          },
//          gensymCount: &count,
//      }
//  })
// DescribeTable("Simple Match Tests", func(me MatchExp, inst string, tables map[string]string, refs map[string]bool) {
//  results, err := me.MatchGenerate().Instantiate(args)
//  Expect(err).To(BeNil())
//  Expect(results.Exp).To(Equal(inst))
//  Expect(results.Tables).To(Equal(tables))
// },
//  Entry(
//      "One table",
//      Match("Deployment", "foo", Namespace("wego-system"), LT(Field("spec", "replicas"), Number(2))),
//      "(foo.NAMESPACE = 'wego-system') AND (json_extract(foo.data, '$.spec.replicas') < 2)",
//      map[string]string{"foo": "Resources"},
//      emptyRefs,
//  ))
// DescribeTable("Conditions Tests", func(ce ConditionsExp, inst string, tables map[string]string, refs map[string]bool) {
//  results, err := ce.ConditionsGenerate().Instantiate(args)
//  Expect(err).To(BeNil())
//  Expect(results.Exp).To(Equal(inst))
//  Expect(results.Tables).To(Equal(tables))
// },
//  Entry(
//      "Two tables",
//      Conditions(
//          Match("Deployment", "foo", Namespace("wego-system"), LT(Field("spec", "replicas"), Number(2))),
//          Match("Deployment", "bar", Namespace("wego-system"), EQ(Field("spec", "replicas"), JoinField("foo", "spec", "replicas")))),
//      `SELECT json_array(0, json_array(foo.ID, bar.ID, json("[]"))) FROM Resources foo JOIN Resources bar ON foo.Kind = 'Deployment' AND bar.Kind = 'Deployment' AND ((foo.NAMESPACE = 'wego-system') AND (json_extract(foo.data, '$.spec.replicas') < 2)) AND ((bar.NAMESPACE = 'wego-system') AND (json_extract(bar.data, '$.spec.replicas') = json_extract(foo.data, '$.spec.replicas')))`,
//      map[string]string{"foo": "Resources", "bar": "Resources"},
//      emptyRefs,
//  ))
//})

var testData string = `
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Ball', 'foo', 'test', '{"kind": "Ball", "namespace": "test", "name": "foo", "color": "red", "size": 10}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Ball', 'bar', 'test', '{"kind": "Ball", "namespace": "test", "name": "bar", "color": "blue", "size": 20}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Cube', 'baz', 'test', '{"kind": "Cube", "namespace": "test", "name": "baz", "color": "blue", "size": 30}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Cube', 'quux', 'test', '{"kind": "Cube", "namespace": "test", "name": "quux", "color": "green", "size": 20}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Cube', 'razz', 'test', '{"kind": "Cube", "namespace": "test", "name": "baz", "color": "red", "size": 40}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Cylinder', 'wanda', 'test', '{"kind": "Cylinder", "namespace": "test", "name": "wanda", "color": "red", "size": 40}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Cylinder', 'groz', 'test', '{"kind": "Cylinder", "namespace": "test", "name": "groz", "color": "green", "size": 10}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Pyramid', 'foom', 'test', '{"kind": "Pyramid", "namespace": "test", "name": "foom", "color": "red", "size": 30}')
INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES ('Pyramid', 'gorp', 'test', '{"kind": "Pyramid", "namespace": "test", "name": "gorp", "color": "green", "size": 10}')
INSERT INTO instantiations (ruleNum, resources) VALUES (0, json_array(1, 4))
`

func NewTestEngine() (*Engine, error) {
	db, err := getTestDB()
	if err != nil {
		return nil, err
	}

	return &Engine{DB: db, KeyFunction: defaultKeyFunc}, nil
}

func getTestDB() (*sql.DB, error) {
	db, err := getDB()
	Expect(err).To(BeNil())

	entries := strings.Split(testData, "\n")
	//	fmt.Printf("ENTRIES: %v, LEN: %d\n", entries, len(entries))

	for _, entry := range entries {
		if entry == "" {
			continue
		}

		s, err := db.Prepare(entry)
		fmt.Printf("ENTRY: %s\n", entry)
		if err != nil {
			return nil, err
		}

		_, err = s.Exec()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

type riEntry struct {
	resourceID, instantiationID int
}

func deleteResource(db *sql.DB, id int) error {
	stmt, err := db.Prepare("DELETE FROM resources WHERE ID = ?")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(id)
	return err
}

func getResourceInstantiationConnections(db *sql.DB) ([]riEntry, error) {
	rows, err := db.Query("SELECT resource_ID, instantiation_ID FROM resource_instantiations")
	defer rows.Close()
	Expect(err).ShouldNot(HaveOccurred())

	var entry riEntry

	entries := []riEntry{}

	for rows.Next() {
		rows.Scan(&entry.resourceID, &entry.instantiationID)
		entries = append(entries, entry)
	}

	rows.Close()

	return entries, nil
}
