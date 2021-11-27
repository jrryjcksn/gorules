package rules

import (
	"database/sql"
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

var (
	emptyTables = map[string]string{}
	emptyRefs   = map[string]bool{}
)

type Table struct {
	Name, BackingName string
}

type ActionFunc func(args []interface{}) error

type InstantiationArgs struct {
	Name           string
	RuleIndex      int
	Priority       int
	KindsToActions map[string]ActionFunc
	Tables         map[string]string
	gensymCount    *int
}

type Queries struct {
	Insert, Update string
}

type InstantiationResults struct {
	RuleIndex int
	Priority  int
	Exp       string
	Refs      map[string]bool
	Tables    map[string]string
	Fields    map[string][]string
	Queries   map[string]Queries
}

type Kind string

type Query string

type RuleName string

type RuleIndex int

type KeyFunc func(jstr string) (string, string, string, error)

func defaultKeyFunc(jstr string) (string, string, string, error) {
	var res map[string]interface{}

	if err := json.Unmarshal([]byte(jstr), &res); err != nil {
		return "", "", "", err
	}

	return res["kind"].(string), res["name"].(string), res["namespace"].(string), nil
}

type Engine struct {
	DB          *sql.DB
	RuleCount   int
	KeyFunction KeyFunc
}

func NewEngine() (*Engine, error) {
	db, err := getDB()
	if err != nil {
		return nil, err
	}

	return &Engine{DB: db, KeyFunction: defaultKeyFunc}, nil
}

func (e *Engine) GetResource(kind, name, namespace string) (string, error) {
	var id int
	var data string

	err := e.DB.QueryRow("SELECT ID, DATA FROM resources WHERE KIND = ? AND NAME = ? AND NAMESPACE = ?", kind, name, namespace).Scan(&id, &data)
	if err != nil {
		return "", err
	}

	//	fmt.Printf("ID: %d\n", id)
	return data, nil
}

func (e *Engine) AddResourceList(resources ...interface{}) error {
	rstrings := []string{}

	for _, r := range resources {
		jstr, err := json.Marshal(r)
		if err != nil {
			return err
		}

		rstrings = append(rstrings, string(jstr))
	}

	return e.AddResourceStringList(rstrings)
}

const insertSQL = "INSERT INTO resources (KIND, NAME, NAMESPACE, DATA) VALUES (?, ?, ?, ?) ON CONFLICT DO UPDATE SET DATA = excluded.DATA"

func (e *Engine) AddResourceStringList(resourceStrs []string) error {
	tx, err := e.DB.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(insertSQL)

	defer stmt.Close()

	for _, rstr := range resourceStrs {
		kind, name, namespace, err := e.KeyFunction(rstr)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(kind, name, namespace, rstr)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (i InstantiationArgs) PeekGensym() string {
	return fmt.Sprintf("%s%d", i.Tables[i.Name], *i.gensymCount)
}

func (i InstantiationArgs) LastGensym() string {
	return fmt.Sprintf("%s%d", i.Tables[i.Name], *i.gensymCount-1)
}

func (i InstantiationArgs) Gensym() string {
	val := fmt.Sprintf("%s%d", i.Tables[i.Name], *i.gensymCount)
	*i.gensymCount++
	return val
}

func (i InstantiationArgs) NamedGensym(name string) string {
	val := fmt.Sprintf("%s%d", name, *i.gensymCount)
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

type ConditionsExp interface {
	ConditionsGenerate() Instantiable
}

type MatchExp interface {
	MatchGenerate() Instantiable
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
	IterableValueGenerate() Instantiable
}

type IterableKeyExp interface {
	IterableKeyGenerate() Instantiable
}
type IterableObjectExp interface {
	IterableObjectGenerate() Instantiable
}
type TestExp interface {
	TestGenerate() Instantiable
}

type ActionsVal struct {
}

type RuleVal struct {
	Name       string
	Priority   int
	Actions    ActionFunc
	Conditions ConditionsVal
	Queries    map[string]Queries
	Indices    []string
}

type AttributeVal struct {
	Key   string
	Value interface{}
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
	Name string
	Path []string
}

type JoinFieldVal struct {
	Name string
	Path []string
}
type ArrayVal struct {
	Array []LiteralValueExp
}
type ObjectVal struct {
	Attributes []AttributeVal
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

type MatchVal struct {
	Kind  string
	Name  string
	Tests []Instantiable
}

type ConditionsVal struct {
	MatchVals []MatchVal
	Matches   []Instantiable
}

type NamespaceVal struct {
	Name string
}

func (n NamespaceVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{
			Exp:    fmt.Sprintf("%s.NAMESPACE = '%s'", args.Name, n.Name),
			Tables: emptyTables,
			Refs:   emptyRefs,
		}, nil
	}}
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
			Refs:   mergeBoolMaps(leftResults.Refs, rightResults.Refs),
			Tables: emptyTables,
			Fields: mergeStringSliceMaps(leftResults.Fields, rightResults.Fields),
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
			Refs:   mergeBoolMaps(leftResults.Refs, rightResults.Refs),
			Tables: emptyTables,
			Fields: mergeStringSliceMaps(leftResults.Fields, rightResults.Fields),
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
			Refs:   mergeBoolMaps(leftResults.Refs, rightResults.Refs),
			Tables: emptyTables,
			Fields: mergeStringSliceMaps(leftResults.Fields, rightResults.Fields),
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
			Tables: emptyTables,
		}, nil
	}}
}

func (s StringVal) LiteralValue() interface{} {
	return s.Str
}

func (s StringVal) ComparableGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{Exp: fmt.Sprintf("'%s'", s.Str), Refs: emptyRefs, Tables: emptyTables}, nil
	}}
}

func (b BoolVal) LiteralValue() interface{} {
	return b.Bit
}

func (b BoolVal) ComparableGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{Exp: fmt.Sprintf("%t", b.Bit), Refs: emptyRefs, Tables: emptyTables}, nil
	}}
}

func (n NumberVal) LiteralValue() interface{} {
	return n.Num
}

func (n NumberVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{Exp: fmt.Sprintf("%G", n.Num), Refs: emptyRefs, Tables: emptyTables}, nil
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

func Object(items ...AttributeVal) ObjectVal {
	return ObjectVal{
		Attributes: items,
	}
}

func Attribute(key string, value LiteralValueExp) AttributeVal {
	return AttributeVal{Key: key, Value: value.LiteralValue()}
}

func Namespace(name string) NamespaceVal {
	return NamespaceVal{Name: name}
}

func Match(kind, name string, tests ...TestExp) MatchVal {
	testVals := []Instantiable{}

	for _, test := range tests {
		testVals = append(testVals, test.TestGenerate())
	}

	return MatchVal{Kind: kind, Name: name, Tests: testVals}
}

type RuleArg func(rv *RuleVal)

func Name(n string) RuleArg {
	return func(rv *RuleVal) {
		rv.Name = n
	}
}

func Priority(n int) RuleArg {
	return func(rv *RuleVal) {
		rv.Priority = n
	}
}

func Rule(args ...RuleArg) RuleVal {
	var rv RuleVal

	for _, arg := range args {
		arg(&rv)
	}

	return rv
}

func (r RuleVal) Instantiate(args InstantiationArgs) (InstantiationResults, error) {
	cres, err := r.Conditions.ConditionsGenerate().Instantiate(args)
	if err != nil {
		return InstantiationResults{}, nil
	}

	res := cres
	res.Queries = map[string]Queries{}
	res.Queries[""] = Queries{Insert: cres.Exp}
	fmt.Printf("\nGEN: %s\n", cres.Exp)

	for _, mv := range r.Conditions.MatchVals {
		n := mv.Name
		res.Queries[n] = Queries{Insert: fmt.Sprintf("CREATE TRIGGER %s_resources_%d AFTER INSERT ON resources WHEN NEW.KIND = '%s' BEGIN %s AND %s.ID = NEW.ID; END", n, args.RuleIndex, mv.Kind, cres.Exp, n)}
	}

	return res, nil
}

func Actions(rhs ActionFunc) RuleArg {
	return func(rv *RuleVal) {
		rv.Actions = rhs
	}
}

func Conditions(matchVals ...MatchVal) RuleArg {
	matches := []Instantiable{}

	for _, match := range matchVals {
		matches = append(matches, match.MatchGenerate())
	}

	return func(rv *RuleVal) {
		rv.Conditions = ConditionsVal{MatchVals: matchVals, Matches: matches}
	}
}

func (m MatchVal) MatchGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		if len(m.Tests) == 0 {
			return InstantiationResults{
				Exp:    "",
				Refs:   emptyRefs,
				Tables: emptyTables,
			}, nil
		}

		targs := args
		targs.Name = m.Name

		inst, err := m.Tests[0].Instantiate(targs)
		if err != nil {
			return InstantiationResults{}, err
		}

		testExp := inst.Exp
		refs := inst.Refs
		tables := inst.Tables
		fields := inst.Fields

		for _, test := range m.Tests[1:] {
			inst, err := test.Instantiate(targs)
			if err != nil {
				return InstantiationResults{}, err
			}

			testExp = fmt.Sprintf("(%s) AND %s", testExp, inst.Exp)
			refs = mergeBoolMaps(refs, inst.Refs)
			tables = mergeStringMaps(tables, inst.Tables)
			fields = mergeStringSliceMaps(fields, inst.Fields)
		}

		return InstantiationResults{
			Exp:    testExp,
			Refs:   refs,
			Tables: tables,
			Fields: fields,
		}, nil
	}}
}

// func getTableForKind(args InstantiationArgs, kind string) string {
//  if tab, ok := args.KindsToTables[kind]; ok {
//      return tab
//  }

//  return args.KindsToTables["*"]
// }

func (c ConditionsVal) ConditionsGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		if len(c.Matches) == 0 {
			return InstantiationResults{
				Exp:    "",
				Refs:   emptyRefs,
				Tables: emptyTables,
			}, nil
		}

		inst, err := c.Matches[0].Instantiate(args)
		if err != nil {
			return InstantiationResults{}, err
		}

		matchExp := inst.Exp
		refs := inst.Refs
		tables := inst.Tables
		fields := inst.Fields

		for _, match := range c.Matches[1:] {
			inst, err := match.Instantiate(args)
			if err != nil {
				return InstantiationResults{}, err
			}

			matchExp = fmt.Sprintf("(%s) AND (%s)", matchExp, inst.Exp)
			refs = mergeBoolMaps(refs, inst.Refs)
			tables = mergeStringMaps(tables, inst.Tables)
			fields = mergeStringSliceMaps(fields, inst.Fields)
		}

		return InstantiationResults{
			Exp:    selectExp(args.RuleIndex, args.Priority, tables, c.MatchVals, matchExp),
			Refs:   refs,
			Tables: tables,
			Fields: fields,
		}, nil
	}}
}

func selectExp(ruleIndex, priority int, tableMap map[string]string, matches []MatchVal, matchExp string) string {
	var tables strings.Builder

	tables.WriteString(fmt.Sprintf(" FROM resources %s", matches[0].Name))

	for _, match := range matches[1:] {
		tables.WriteString(fmt.Sprintf(", resources %s", match.Name))
	}

	var kinds strings.Builder

	kinds.WriteString(fmt.Sprintf(" WHERE %s.KIND = '%s'", matches[0].Name, matches[0].Kind))

	for _, match := range matches[1:] {
		kinds.WriteString(fmt.Sprintf(" AND %s.KIND = '%s'", match.Name, match.Kind))
	}

	var args strings.Builder

	args.WriteString(fmt.Sprintf("%s.ID", matches[0].Name))

	for _, match := range matches[1:] {
		args.WriteString(fmt.Sprintf(", %s.ID", match.Name))
	}

	return fmt.Sprintf(`INSERT INTO instantiations (ruleNum, priority, resources) SELECT %d, %d, json_array(%s)%s%s AND %s`, ruleIndex, priority, args.String(), tables.String(), kinds.String(), matchExp)
}

func (o ObjectVal) IterableObjectGenerate() Instantiable {
	return Instantiable{InstFunc: func(_ InstantiationArgs) (InstantiationResults, error) {
		val := o.LiteralValue()

		strval, err := json.Marshal(val)
		if err != nil {
			return InstantiationResults{}, err
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("select json_object(key, value) from json_each('%s')", strval),
			Refs:   emptyRefs,
			Tables: emptyTables,
		}, nil
	},
	}
}

func (a ArrayVal) IterableValueGenerate() Instantiable {
	return Instantiable{InstFunc: func(_ InstantiationArgs) (InstantiationResults, error) {
		val := a.LiteralValue()

		strval, err := json.Marshal(val)
		if err != nil {
			return InstantiationResults{}, err
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("select value from json_each('%s')", strval),
			Refs:   emptyRefs,
			Tables: emptyTables,
		}, nil
	},
	}

}

func (a ArrayVal) IterableKeyGenerate() Instantiable {
	return Instantiable{InstFunc: func(_ InstantiationArgs) (InstantiationResults, error) {
		val := a.LiteralValue()

		strval, err := json.Marshal(val)
		if err != nil {
			return InstantiationResults{}, err
		}

		return InstantiationResults{
			Exp:    fmt.Sprintf("select key from json_each('%s')", strval),
			Refs:   emptyRefs,
			Tables: emptyTables,
		}, nil
	},
	}
}

func (f FieldVal) NumericGenerate() Instantiable {
	path := strings.Join(f.Path, ".")

	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		exp := fmt.Sprintf("json_extract(%s.DATA, '$.%s')", args.Name, path)

		return InstantiationResults{
			Exp:    exp,
			Refs:   emptyRefs,
			Tables: emptyTables,
			Fields: map[string][]string{args.Name: []string{exp}},
		}, nil
	},
	}
}

func (f FieldVal) ComparableGenerate() Instantiable {
	return f.NumericGenerate()
}

func (f FieldVal) IterableValueGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		baseTableName := args.Tables[args.Name]
		name := args.Gensym()
		eachName := args.NamedGensym("each")

		return InstantiationResults{
			Exp: fmt.Sprintf("select %s.value from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
				eachName, baseTableName, name, name, strings.Join(f.Path, "."), eachName, name, baseTableName),
			Refs:   emptyRefs,
			Tables: emptyTables,
			Fields: map[string][]string{},
		}, nil
	},
	}
}

func (f FieldVal) IterableKeyGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		baseTableName := args.Tables[args.Name]
		name := args.Gensym()
		eachName := args.NamedGensym("each")

		return InstantiationResults{
			Exp: fmt.Sprintf("select %s.key from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
				eachName, baseTableName, name, name, strings.Join(f.Path, "."), eachName, name, baseTableName),
			Refs:   emptyRefs,
			Tables: emptyTables,
			Fields: map[string][]string{},
		}, nil
	},
	}
}

func (f FieldVal) IterableObjectGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		baseTableName := args.Tables[args.Name]
		name := args.Gensym()
		eachName := args.NamedGensym("each")

		return InstantiationResults{
			Exp: fmt.Sprintf("select json_object(%s.key, %s.value) from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
				eachName, eachName, baseTableName, name, name, strings.Join(f.Path, "."), eachName, name, baseTableName),
			Refs:   emptyRefs,
			Tables: emptyTables,
			Fields: map[string][]string{},
		}, nil
	},
	}
}

func (j JoinFieldVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		return InstantiationResults{
			Exp:    fmt.Sprintf("json_extract(%s.DATA, '$.%s')", j.Name, strings.Join(j.Path, ".")),
			Refs:   map[string]bool{j.Name: true},
			Tables: emptyTables,
			Fields: map[string][]string{},
		}, nil
	},
	}
}

func (j JoinFieldVal) ComparableGenerate() Instantiable {
	return j.NumericGenerate()
}

func (j JoinFieldVal) IterableValueGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		baseTableName := args.Tables[j.Name]
		name := args.NamedGensym(baseTableName)
		eachName := args.NamedGensym("each")

		return InstantiationResults{
			Exp: fmt.Sprintf("select %s.value from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
				eachName, baseTableName, name, name, strings.Join(j.Path, "."), eachName, name, baseTableName),
			Refs:   emptyRefs,
			Tables: emptyTables,
		}, nil
	},
	}
}

func (j JoinFieldVal) IterableKeyGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		baseTableName := args.Tables[j.Name]
		name := args.NamedGensym(baseTableName)
		eachName := args.NamedGensym("each")

		return InstantiationResults{
			Exp: fmt.Sprintf("select %s.key from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
				eachName, baseTableName, name, name, strings.Join(j.Path, "."), eachName, name, baseTableName),
			Refs:   emptyRefs,
			Tables: emptyTables,
			Fields: map[string][]string{},
		}, nil
	},
	}
}

func (j JoinFieldVal) IterableObjectGenerate() Instantiable {
	return Instantiable{InstFunc: func(args InstantiationArgs) (InstantiationResults, error) {
		baseTableName := args.Tables[j.Name]
		name := args.NamedGensym(baseTableName)
		eachName := args.NamedGensym("each")

		return InstantiationResults{
			Exp: fmt.Sprintf("select json_object(%s.key, %s.value) from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
				eachName, eachName, baseTableName, name, name, strings.Join(j.Path, "."), eachName, name, baseTableName),
			Refs:   emptyRefs,
			Tables: emptyTables,
			Fields: map[string][]string{},
		}, nil
	},
	}
}

func (a ArrayVal) LiteralValue() interface{} {
	items := []interface{}{}

	for _, item := range a.Array {
		items = append(items, item.LiteralValue())
	}

	return items
}

func (o ObjectVal) LiteralValue() interface{} {
	items := map[string]interface{}{}

	for _, item := range o.Attributes {
		items[item.Key] = item.Value
	}

	return items
}

func mergeBoolMaps(m1, m2 map[string]bool) map[string]bool {
	m3 := map[string]bool{}

	for k, v := range m1 {
		m3[k] = v
	}

	for k, v := range m2 {
		m3[k] = v
	}

	return m3
}

func mergeStringMaps(m1, m2 map[string]string) map[string]string {
	m3 := map[string]string{}

	for k, v := range m1 {
		m3[k] = v
	}

	for k, v := range m2 {
		m3[k] = v
	}

	return m3
}

func mergeStringSliceMaps(m1, m2 map[string][]string) map[string][]string {
	m3 := map[string][]string{}

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

baz.y.q CONTAINS bar.x = NOT(EXISTS(SELECT(true FROM bar, json_each(bar.DATA, '$.x') j1 WHERE bar.ID = <BAR ID> AND j1.value NOT IN(SELECT j2.value FROM baz, json_each(baz.DATA, '$.y.q') WHERE baz.ID = <BAZ ID>)
func Rule(name string, query ruleQuery, actions ...action) error {
    rule :=
        Rule{
            priority: defaultPriority,
            query:    query, // `SELECT key, DATA FROM Resources WHERE kind = 'Deployment' AND NAMESPACE = 'my-controller' AND json_extract(Resources, '$.spec.replicas') < 2`,
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
