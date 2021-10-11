package rules

import (
	"fmt"
	"strings"
)

type NumericComparisonOperator string

const (
	LessThan           NumericComparisonOperator = "<"
	GreaterThan        NumericComparisonOperator = ">"
	LessThanOrEqual    NumericComparisonOperator = "<="
	GreaterThanOrEqual NumericComparisonOperator = ">="
)

type InstantiationFunction func(name string) (string, map[string]bool)

type Instantiable struct {
	InstFunc InstantiationFunction
}

func (i Instantiable) Instantiate(name string) (string, map[string]bool) {
	return i.InstFunc(name)
}

type NumericValueExp interface {
	NumericGenerate() Instantiable
}

type TestExp interface {
	TestGenerate() Instantiable
}

type NumberVal struct {
	Num float64
}

type FieldVal struct {
	Path []string
}

type JoinFieldVal struct {
	Name string
	Path []string
}

type NumericBinaryTestVal struct {
	Op    NumericComparisonOperator
	Left  Instantiable
	Right Instantiable
}

func (n NumericBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(name string) (string, map[string]bool) {
		leftStr, leftRefs := n.Left.Instantiate(name)
		rightStr, rightRefs := n.Right.Instantiate(name)
		return fmt.Sprintf("%s %s %s", leftStr, n.Op, rightStr), mergeMaps(leftRefs, rightRefs)
	}}
}

func (n NumberVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(name string) (string, map[string]bool) {
		return fmt.Sprintf("%G", n.Num), map[string]bool{}
	}}
}

func LT(left, right NumericValueExp) NumericBinaryTestVal {
	return NumericBinaryTestVal{
		Op:    LessThan,
		Left:  left.NumericGenerate(),
		Right: right.NumericGenerate(),
	}
}

func Number(n float64) NumberVal {
	return NumberVal{Num: n}
}

func Field(path ...string) FieldVal {
	return FieldVal{Path: path}
}

func JoinField(objectName string, path ...string) JoinFieldVal {
	return JoinFieldVal{Name: objectName, Path: path}
}

func (f FieldVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(name string) (string, map[string]bool) {
		return fmt.Sprintf("json_extract(%s.data, '$.%s')", name, strings.Join(f.Path, ".")), map[string]bool{}
	},
	}
}

func (j JoinFieldVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(name string) (string, map[string]bool) {
		return fmt.Sprintf("json_extract(%s.data, '$.%s')", j.Name, strings.Join(j.Path, ".")), map[string]bool{j.Name: true}
	},
	}
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

/* type ObjectRef {
	Name string
	Expression Exp
}

type Exp interface {
	Instantiate(name string) (string, []string)
}

type NumberExp struct {
	Value: float64
}

type FieldExp struct {
	Path []string
}

type Numeric interface {
	InstantiateNumber(name string) (string, []string)
}

func (i NumberExp) Instantiate(_ string) (string, []string) {
	return fmt.Sprintf("%G", i), []string{}
}

func (i NumberExp) InstantiateNumber(_ string) (string, []string) {
	return fmt.Sprintf("%G", i), []string{}
}

func (i FieldExp) Instantiate(name string) (string, []string) {
	return fmt.Sprintf("%s.%s", i), []string{}
}

func (i NumberExp) InstantiateNumber(_ string) (string, []string) {
	return fmt.Sprintf("%G", i), []string{}
}

func Number(float64 n) NumberExp {
	return NumberExp{Value: n}
}

func init() {
	Rule("rule1",
		Tests(
			Object("dep", Kind("Deployment").Namespace("my-controller").LT(Field("spec", "replicas"), Int(2))),
		Actions(
			Alert("Insufficient replicas for deployment: %s", Object("dep").Field("Name")),
			Modify("dep", Field("spec", "replicas"), 2)))
}

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

func Object(name string, exp ExpFunc) ObjectRef {
	return ObjectRef{Name: name, Expression: exp(name)}
}

func (ef ExpFunc) Kind(kind string) ExpFunc {
	return func(name string) string {
		return fmt.Sprintf("((%s) AND %s.Kind = '%s')", ef(name), name, kind)
	}
}

func (ef ExpFunc) Namespace(ns string) ExpFunc {
	return func(name string) string {}
		return fmt.Sprintf("((%s) AND %s.Namespace = '%s')", ef(name), name, ns)
	}
}


func Join(name string, path ...string) ExpFunc {
	return func(_ string) string {
		return FieldRef{Name: name, Path: path}
	}
}

func (or ObjectRef) OR(or1 ObjectRef, or2 ObjectRef) ObjectRef {
	return ObjectRef{
		Name: or.Name,
		Expression: fmt.Sprintf("(%s) AND ((%s) OR (%s))", or.Expression, or1.Expression, or2.Expression)
	}
}

func LT(field FieldRef, val float64) ObjectRef {
	return Expression{
		expStr: fmt.Sprintf("%s < %v", field, val)
	}
}

func GT(field FieldRef, val float64) ObjectRef {
	return Expression{
		expStr: fmt.Sprintf("%s > %v", field, val)
	}
}
func LE(field FieldRef, val float64) ObjectRef {
	return Expression{
		expStr: fmt.Sprintf("%s <= %v", field, val)
	}
}

func GE(field FieldRef, val float64) ObjectRef {
	return Expression{
		expStr: fmt.Sprintf("%s >= %v", field, val)
	}
}

func EQ(field FieldRef, val float64) ObjectRef {
	return Expression{
		expStr: fmt.Sprintf("%s = %v", field, val)
	}
}

func NEQ(field FieldRef, val float64) ObjectRef {
	return Expression{
		expStr: fmt.Sprintf("%s <> %v", field, val)
	}
}


var action1 = func(objs map[string]map[string]interface{}) error {
	return AlertImpl("Insufficient replicas for deployment: %s",
		func() (string, error) {
			fieldVal0, err0 := FieldImpl(objs, "dep", []string{"Name"})
			if err0 != nil {
				return "", err0
			}

			return fmt.Sprintf("%v", fieldVal0), nil
		})
}

var action2 = func(objs map[string]map[string]interface{}) error {
	return ModifyImpl(objs[name], []string{"spec", "replicas"}, 2)
}

func ModifyImpl(obj map[string]interface{}, path []string, val interface{}) error {
	if dberr := modifyDB(obj, path, val); dberr != nil {
		return dberr
	}

	if k8serr := modifyK8s(obj, path, val); k8serr != nil {
		return k8serr
	}

	return nil
}
*/
