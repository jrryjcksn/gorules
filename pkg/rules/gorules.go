package rules

import (
	"encoding/json"
	"fmt"
	"strings"
)

type NumericComparisonOperator string

type ComparableComparisonOperator string

type TestComparisonOperator string

type UnaryTestOperator string

const NotOp = "NOT"

const (
	LessThan           NumericComparisonOperator = "<"
	GreaterThan        NumericComparisonOperator = ">"
	LessThanOrEqual    NumericComparisonOperator = "<="
	GreaterThanOrEqual NumericComparisonOperator = ">="
)

const (
	IsEqual    ComparableComparisonOperator = "="
	IsNotEqual ComparableComparisonOperator = "<>"
)

const (
	AndOp TestComparisonOperator = "AND"
	OrOp  TestComparisonOperator = "OR"
)

type Table struct {
	Name, BackingName string
}
type InstantiationArgs struct {
	Name        string
	gensymCount *int
}

type InstantiationResults struct {
	Exp    string
	Refs   map[string]bool
	Tables []Table
}

func (i InstantiationArgs) Gensym() string {
	val := fmt.Sprintf("%s%d", i.Name, i.gensymCount)
	*i.gensymCount++
	return val
}

type InstantiationFunction func(args InstantiationArgs) (InstantiationResults, error)

type Instantiable struct {
	InstFunc InstantiationFunction
}

func (i Instantiable) Instantiate(args InstantiationArgs) (InstantiationResults, error) {
	return i.InstFunc(args)
}

type LiteralValueExp interface {
	LiteralValue() interface{}
}

type NumericValueExp interface {
	NumericGenerate() Instantiable
}

type ComparableValueExp interface {
	ComparableGenerate() Instantiable
}
type IterableValueExp interface {
	IterableGenerate() Instantiable
}
type TestExp interface {
	TestGenerate() Instantiable
}

type StringVal struct {
	Str string
}

type NumberVal struct {
	Num float64
}

type BoolVal struct {
	Bit bool
}

type FieldVal struct {
	Path []string
}

type JoinFieldVal struct {
	Name string
	Path []string
}
type ArrayVal struct {
	Array []LiteralValueExp
}
type NumericBinaryTestVal struct {
	Op    NumericComparisonOperator
	Left  Instantiable
	Right Instantiable
}

type ComparableBinaryTestVal struct {
	Op    ComparableComparisonOperator
	Left  Instantiable
	Right Instantiable
}

type TestBinaryTestVal struct {
	Op    TestComparisonOperator
	Left  Instantiable
	Right Instantiable
}

type UnaryTestVal struct {
	Op  UnaryTestOperator
	Arg Instantiable
}

func (n NumericBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		leftResults, leftError := n.Left.Instantiate(args)
		if leftError != nil {
			return InstantiationResults{}, leftError
		}

		rightResults, rightError := n.Right.Instantiate(args)
		if rightError != nil {
			return InstantiationResults{}, rightError
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("%s %s %s", leftResults.Exp, n.Op, rightResults.Exp),
			Refs:   mergeMaps(leftResults.Refs, rightResults.Refs),
			Tables: []Table{},
		}, nil
	}}
}

func (c ComparableBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		leftResults, leftError := c.Left.Instantiate(args)
		if leftError != nil {
			return InstantiationResults{}, leftError
		}

		rightResults, rightError := c.Right.Instantiate(args)
		if rightError != nil {
			return InstantiationResults{}, rightError
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("%s %s %s", leftResults.Exp, c.Op, rightResults.Exp),
			Refs:   mergeMaps(leftResults.Refs, rightResults.Refs),
			Tables: []Table{},
		}, nil
	}}
}

func (t TestBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		leftResults, leftError := t.Left.Instantiate(args)
		if leftError != nil {
			return InstantiationResults{}, leftError
		}

		rightResults, rightError := t.Right.Instantiate(args)
		if rightError != nil {
			return InstantiationResults{}, rightError
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("(%s) %s (%s)", leftResults.Exp, t.Op, rightResults.Exp),
			Refs:   mergeMaps(leftResults.Refs, rightResults.Refs),
			Tables: []Table{},
		}, nil
	}}
}

func (u UnaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		argResults, err := u.Arg.Instantiate(args)
		if err != nil {
			return InstantiationResults{}, err
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("%s(%s)", u.Op, argResults.Exp),
			Refs:   argResults.Refs,
			Tables: []Table{},
		}, nil
	}}
}

func (s StringVal) LiteralValue() interface{} {
	return s.Str
}

func (s StringVal) ComparableGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{Exp: fmt.Sprintf("'%s'", s.Str), Refs: map[string]bool{}, Tables: []Table{}}, nil
	}}
}

func (b BoolVal) LiteralValue() interface{} {
	return b.Bit
}

func (b BoolVal) ComparableGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{Exp: fmt.Sprintf("%t", b.Bit), Refs: map[string]bool{}, Tables: []Table{}}, nil
	}}
}

func (n NumberVal) LiteralValue() interface{} {
	return n.Num
}

func (n NumberVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{Exp: fmt.Sprintf("%G", n.Num), Refs: map[string]bool{}, Tables: []Table{}}, nil
	}}
}

func (n NumberVal) ComparableGenerate() Instantiable {
	return n.NumericGenerate()
}

func LT(left, right NumericValueExp) NumericBinaryTestVal {
	return NumericBinaryTestVal{
		Op:    LessThan,
		Left:  left.NumericGenerate(),
		Right: right.NumericGenerate(),
	}
}

func LE(left, right NumericValueExp) NumericBinaryTestVal {
	return NumericBinaryTestVal{
		Op:    LessThanOrEqual,
		Left:  left.NumericGenerate(),
		Right: right.NumericGenerate(),
	}
}

func GT(left, right NumericValueExp) NumericBinaryTestVal {
	return NumericBinaryTestVal{
		Op:    GreaterThan,
		Left:  left.NumericGenerate(),
		Right: right.NumericGenerate(),
	}
}

func GE(left, right NumericValueExp) NumericBinaryTestVal {
	return NumericBinaryTestVal{
		Op:    GreaterThanOrEqual,
		Left:  left.NumericGenerate(),
		Right: right.NumericGenerate(),
	}
}

func EQ(left, right ComparableValueExp) ComparableBinaryTestVal {
	return ComparableBinaryTestVal{
		Op:    IsEqual,
		Left:  left.ComparableGenerate(),
		Right: right.ComparableGenerate(),
	}
}

func NEQ(left, right ComparableValueExp) ComparableBinaryTestVal {
	return ComparableBinaryTestVal{
		Op:    IsNotEqual,
		Left:  left.ComparableGenerate(),
		Right: right.ComparableGenerate(),
	}
}

func AND(left, right TestExp) TestBinaryTestVal {
	return TestBinaryTestVal{
		Op:    AndOp,
		Left:  left.TestGenerate(),
		Right: right.TestGenerate(),
	}
}

func OR(left, right TestExp) TestBinaryTestVal {
	return TestBinaryTestVal{
		Op:    OrOp,
		Left:  left.TestGenerate(),
		Right: right.TestGenerate(),
	}
}

func NOT(arg TestExp) UnaryTestVal {
	return UnaryTestVal{
		Op:  NotOp,
		Arg: arg.TestGenerate(),
	}
}

func String(s string) StringVal {
	return StringVal{Str: s}
}

func Number(n float64) NumberVal {
	return NumberVal{Num: n}
}

func Bool(b bool) BoolVal {
	return BoolVal{Bit: b}
}

func Field(path ...string) FieldVal {
	return FieldVal{Path: path}
}

func JoinField(objectName string, path ...string) JoinFieldVal {
	return JoinFieldVal{Name: objectName, Path: path}
}

func Array(items ...LiteralValueExp) ArrayVal {
	return ArrayVal{
		Array: items,
	}
}

func (a ArrayVal) IterableGenerate() Instantiable {
	return Instantiable{InstFunc: func(_ InstantiationArgs) (InstantiationResults, error) {
		val, err := toLiteralJsonArray(a.Array)
		if err != nil {
			return InstantiationResults{}, err
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("json_each('%s')", val),
			Refs:   map[string]bool{},
			Tables: []Table{},
		}, nil
	},
	}

}

func (f FieldVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{
			Exp:    fmt.Sprintf("json_extract(%s.data, '$.%s')", args.Name, strings.Join(f.Path, ".")),
			Refs:   map[string]bool{},
			Tables: []Table{},
		}, nil
	},
	}
}

func (f FieldVal) ComparableGenerate() Instantiable {
	return f.NumericGenerate()
}

func (j JoinFieldVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{
			Exp:    fmt.Sprintf("json_extract(%s.data, '$.%s')", j.Name, strings.Join(j.Path, ".")),
			Refs:   map[string]bool{j.Name: true},
			Tables: []Table{},
		}, nil
	},
	}
}

func (j JoinFieldVal) ComparableGenerate() Instantiable {
	return j.NumericGenerate()
}

func toLiteralJsonArray(val []LiteralValueExp) (string, error) {
	items := []interface{}{}

	for _, item := range val {
		items = append(items, item.LiteralValue())
	}

	result, err := json.Marshal(items)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

func mergeMaps(m1, m2 map[string]bool) map[string]bool {
	m3 := map[string]bool{}

	for k, v := range m1 {
		m3[k] = v
	}

	for k, v := range m2 {
		m3[k] = v
	}

	return m3
}

/*
func init() {
	Rule("rule1",
		Tests(
			Object("dep", Kind("Deployment").Namespace("my-controller").LT(Field("spec", "replicas"), Int(2))),
		Actions(
			Alert("Insufficient replicas for deployment: %s", Object("dep").Field("Name")),
			Modify("dep", Field("spec", "replicas"), 2)))
}

baz.y.q CONTAINS bar.x = NOT(EXISTS(SELECT(true FROM bar, json_each(bar.data, '$.x') j1 WHERE bar.ID = <BAR ID> AND j1.value NOT IN(SELECT j2.value FROM baz, json_each(baz.data, '$.y.q') WHERE baz.ID = <BAZ ID>)
func Rule(name string, query ruleQuery, actions ...action) error {
	rule :=
		Rule{
			priority: defaultPriority,
			query:    query, // `SELECT key, data FROM Resources WHERE kind = 'Deployment' AND namespace = 'my-controller' AND json_extract(Resources, '$.spec.replicas') < 2`,
			operation: func(objs map[string]map[string]interface{}) error {
				for action, _ := range actions {
					if err := action(objs); err != nil {
						return err
					}
				}

				return nil
			},
		}

	for _, typ := range query.Types {
		rules[typ] = append(rules[typ], rule)
	}
}
*/
