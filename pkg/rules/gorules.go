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
	rulesets    = map[string]*RuleSetVal{}
)

type RuleSetVal struct {
	Name  string
	Rules []*RuleVal
}

type Table struct {
	Name, BackingName string
}

type ActionFunc func(rc *RuleContext) error

type InstantiationData struct {
	Names     []string
	RuleIndex int
	Priority  int
	Tables    map[string]string
	Refs      map[string]bool
	//	FieldChecks    map[string]map[string]bool
	ObjectMap   map[string]int
	Queries     map[string]Queries
	gensymCount int
}

type Queries struct {
	Insert, Update string
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

	return res["kind"].(string), res["metadata"].(map[string]interface{})["name"].(string), res["metadata"].(map[string]interface{})["namespace"].(string), nil
}

type Engine struct {
	DB              *sql.DB
	RuleNameToIndex map[string]int
	IndexToRuleName map[int]string
	RuleSets        []*RuleSetVal
	RuleCount       int
	RuleFunctions   map[int]ActionFunc
	ObjectMaps      map[int]map[string]int
	KeyFunction     KeyFunc
}

func (q Queries) AddSQL(allQueries []string) []string {
	return append(allQueries, q.Insert)
}

func NewEngine(path string, rulesets ...string) (*Engine, error) {
	db, err := getDB(path)
	if err != nil {
		return nil, err
	}

	eng := &Engine{
		DB:              db,
		KeyFunction:     defaultKeyFunc,
		RuleFunctions:   map[int]ActionFunc{},
		RuleSets:        []*RuleSetVal{},
		ObjectMaps:      map[int]map[string]int{},
		RuleNameToIndex: map[string]int{},
		IndexToRuleName: map[int]string{},
	}

	for _, rsname := range rulesets {
		if err := eng.AddRuleSet(rsname); err != nil {
			return nil, err
		}
	}

	return eng, nil
}

func (e *Engine) AddRuleSet(name string) error {
	rs, ok := rulesets[name]
	if !ok {
		return fmt.Errorf("no such ruleset: %s", name)
	}

	e.RuleSets = append(e.RuleSets, rs)

	allSQL := []string{}
	ruleID := e.RuleCount

	for _, rule := range rs.Rules {
		idata := &InstantiationData{
			Names:     []string{},
			RuleIndex: ruleID,
			Tables:    map[string]string{},
			Refs:      map[string]bool{},
			Queries:   map[string]Queries{},
		}

		if _, err := rule.Instantiate(idata, 0); err != nil {
			return err
		}

		for key, val := range idata.Queries {
			if key != "" {
				allSQL = val.AddSQL(allSQL)
			}
		}

		e.ObjectMaps[ruleID] = idata.ObjectMap
		ruleID++
	}

	if err := e.ApplyInTransaction(allSQL); err != nil {
		return err
	}

	for _, rule := range rs.Rules {
		e.RuleNameToIndex[rule.Name] = e.RuleCount
		e.IndexToRuleName[e.RuleCount] = rule.Name
		e.RuleFunctions[e.RuleCount] = rule.Actions
		e.RuleCount++
	}

	return nil
}

func (e *Engine) Run() error {
	var id int
	var ruleNum int
	var resources string

	instantiationErrors := map[int]int{}

	for {
		tx, err := e.DB.Begin()
		if err != nil {
			return err
		}

		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		err = tx.QueryRow("SELECT instantiations.ID, ruleNum, json_group_array(res.value) FROM instantiations, json_each(instantiations.resources) res GROUP BY instantiations.ID, ruleNum ORDER BY priority, timestamp LIMIT 1").Scan(&id, &ruleNum, &resources)

		switch {
		case err == sql.ErrNoRows:
			return nil
		case err != nil:
			return err
		default:
			var resIds []int

			if err := json.Unmarshal([]byte(resources), &resIds); err != nil {
				return err
			}

			rc := &RuleContext{tx: tx, resourceMap: e.ObjectMaps[ruleNum], resources: resIds}

			if err := e.CallAction(rc, e.RuleFunctions[ruleNum]); err != nil {
				count := instantiationErrors[id]
				if count > 3 {
					return err
				}

				instantiationErrors[id] = count + 1
			} else {
				delete(instantiationErrors, id)
				statement, err := tx.Prepare("DELETE FROM instantiations WHERE ID = ?")
				if err != nil {
					return err
				}

				defer statement.Close()

				_, err = statement.Exec(id)
				if err != nil {
					return nil
				}

				if err := tx.Commit(); err != nil {
					return err
				}
			}
		}
	}
}

type RuleContext struct {
	tx          *sql.Tx
	resourceMap map[string]int
	resources   []int
}

func (rc *RuleContext) GetIntField(objname, path string) (int, error) {
	idx, ok := rc.resourceMap[objname]
	if !ok {
		return 0, fmt.Errorf("unknown object: %s", objname)
	}

	s, err := rc.tx.Prepare("SELECT json_extract(data, ?) FROM Resources WHERE ID = ?")
	if err != nil {
		return 0, err
	}

	var field int

	err = s.QueryRow(path, rc.resources[idx]).Scan(&field)
	if err != nil {
		return 0, err
	}

	return field, nil
}

func (e *Engine) CallAction(rc *RuleContext, action ActionFunc) error {
	return action(rc)
}

func (e *Engine) ApplyInTransaction(sql []string) error {
	for _, q := range sql {
		fmt.Printf("SQL: %s\n", q)
	}

	tx, err := e.DB.Begin()
	if err != nil {
		return err
	}

	for _, str := range sql {
		stmt, err := tx.Prepare(str)

		defer stmt.Close()

		_, err = stmt.Exec()
		if err != nil {
			return err
		}

		stmt.Close()
	}

	return tx.Commit()
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

func (i *InstantiationData) Gensym(matchIndex int) string {
	val := fmt.Sprintf("%s%d", i.Tables[i.Names[matchIndex]], i.gensymCount)
	i.gensymCount++
	return val
}

func (i *InstantiationData) NamedGensym(name string) string {
	val := fmt.Sprintf("%s%d", name, i.gensymCount)
	i.gensymCount++
	return val
}

type InstantiationFunction func(data *InstantiationData, matchIndex int) (string, error)

type Instantiable struct {
	InstFunc InstantiationFunction
}

func (i Instantiable) Instantiate(data *InstantiationData, matchIndex int) (string, error) {
	return i.InstFunc(data, matchIndex)
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
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		return fmt.Sprintf("%s.NAMESPACE = '%s'", data.Names[matchIndex], n.Name), nil
	}}
}

func (n NumericBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		leftExp, leftError := n.Left.Instantiate(data, matchIndex)
		if leftError != nil {
			return "", leftError
		}

		rightExp, rightError := n.Right.Instantiate(data, matchIndex)
		if rightError != nil {
			return "", rightError
		}

		return fmt.Sprintf("%s %s %s", leftExp, n.Op, rightExp), nil
	}}
}

func (c ComparableBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		leftExp, leftError := c.Left.Instantiate(data, matchIndex)
		if leftError != nil {
			return "", leftError
		}

		rightExp, rightError := c.Right.Instantiate(data, matchIndex)
		if rightError != nil {
			return "", rightError
		}

		return fmt.Sprintf("%s %s %s", leftExp, c.Op, rightExp), nil
	}}
}

func (t TestBinaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		leftExp, leftError := t.Left.Instantiate(data, matchIndex)
		if leftError != nil {
			return "", leftError
		}

		rightExp, rightError := t.Right.Instantiate(data, matchIndex)
		if rightError != nil {
			return "", rightError
		}

		return fmt.Sprintf("(%s) %s (%s)", leftExp, t.Op, rightExp), nil
	}}
}

func (u UnaryTestVal) TestGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		argExp, err := u.Arg.Instantiate(data, matchIndex)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s(%s)", u.Op, argExp), nil
	}}
}

func (s StringVal) LiteralValue() interface{} {
	return s.Str
}

func (s StringVal) ComparableGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		return fmt.Sprintf("'%s'", s.Str), nil
	}}
}

func (b BoolVal) LiteralValue() interface{} {
	return b.Bit
}

func (b BoolVal) ComparableGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		return fmt.Sprintf("%t", b.Bit), nil
	}}
}

func (n NumberVal) LiteralValue() interface{} {
	return n.Num
}

func (n NumberVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		return fmt.Sprintf("%G", n.Num), nil
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

func RuleSet(name string, rules ...*RuleVal) {
	rulesets[name] = &RuleSetVal{Name: name, Rules: rules}
}

func Rule(data ...RuleArg) *RuleVal {
	var rv RuleVal

	for _, arg := range data {
		arg(&rv)
	}

	return &rv
}

func (r RuleVal) Instantiate(data *InstantiationData, matchIndex int) (string, error) {
	cexp, err := r.Conditions.ConditionsGenerate().Instantiate(data, matchIndex)
	if err != nil {
		return "", nil
	}

	data.Queries[""] = Queries{Insert: cexp}
	data.ObjectMap = map[string]int{}

	for idx, mv := range r.Conditions.MatchVals {
		n := mv.Name
		data.Queries[n] = Queries{Insert: fmt.Sprintf("CREATE TRIGGER %s_resources_%d AFTER INSERT ON resources WHEN NEW.KIND = '%s' BEGIN %s AND %s.ID = NEW.ID; END", n, data.RuleIndex, mv.Kind, cexp, n)}
		data.ObjectMap[n] = idx
	}

	return cexp, nil
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
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		if len(m.Tests) == 0 {
			return "", nil
		}

		testExp, err := m.Tests[0].Instantiate(data, matchIndex)
		if err != nil {
			return "", err
		}

		for _, test := range m.Tests[1:] {
			iexp, err := test.Instantiate(data, matchIndex)
			if err != nil {
				return "", err
			}

			testExp = fmt.Sprintf("(%s) AND %s", testExp, iexp)
		}

		return testExp, nil
	}}
}

// func getTableForKind(data *InstantiationData, kind string) string {
//  if tab, ok := data.KindsToTables[kind]; ok {
//      return tab
//  }

//  return data.KindsToTables["*"]
// }

func (c ConditionsVal) ConditionsGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		if len(c.Matches) == 0 {
			return "", nil
		}

		for _, m := range c.MatchVals {
			data.Names = append(data.Names, m.Name)
		}

		matchExp, err := c.Matches[0].Instantiate(data, matchIndex)
		if err != nil {
			return "", err
		}

		for idx, match := range c.Matches[1:] {
			iexp, err := match.Instantiate(data, idx+1)
			if err != nil {
				return "", err
			}

			matchExp = fmt.Sprintf("(%s) AND (%s)", matchExp, iexp)
		}

		return selectExp(data.RuleIndex, data.Priority, data.Tables, c.MatchVals, matchExp), nil
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
	return Instantiable{InstFunc: func(_ *InstantiationData, _ int) (string, error) {
		val := o.LiteralValue()

		strval, err := json.Marshal(val)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("select json_object(key, value) from json_each('%s')", strval), nil
	}}
}

func (a ArrayVal) IterableValueGenerate() Instantiable {
	return Instantiable{InstFunc: func(_ *InstantiationData, _ int) (string, error) {
		val := a.LiteralValue()

		strval, err := json.Marshal(val)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("select value from json_each('%s')", strval), nil
	},
	}

}

func (a ArrayVal) IterableKeyGenerate() Instantiable {
	return Instantiable{InstFunc: func(_ *InstantiationData, _ int) (string, error) {
		val := a.LiteralValue()

		strval, err := json.Marshal(val)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("select key from json_each('%s')", strval), nil
	},
	}
}

func (f FieldVal) NumericGenerate() Instantiable {
	path := strings.Join(f.Path, ".")

	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		exp := fmt.Sprintf("json_extract(%s.DATA, '$.%s')", data.Names[matchIndex], path)

		//      name := data.Names[matchIndex]
		// //		nmap := data.FieldChecks[name]

		//      if nmap == nil {
		//          nmap = map[string]bool{}
		//          data.FieldChecks[name] = nmap
		//      }

		//      nmap[fmt.Sprintf("json_extract(NEW.DATA, '$.%s') <> json_extract(OLD.DATA, '$.%s')", path, path)] = true
		return exp, nil
	},
	}
}

func (f FieldVal) ComparableGenerate() Instantiable {
	return f.NumericGenerate()
}

func (f FieldVal) IterableValueGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		baseTableName := data.Tables[data.Names[matchIndex]]
		name := data.Gensym(matchIndex)
		eachName := data.NamedGensym("each")

		return fmt.Sprintf("select %s.value from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
			eachName, baseTableName, name, name, strings.Join(f.Path, "."), eachName, name, baseTableName), nil
	}}
}

func (f FieldVal) IterableKeyGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		baseTableName := data.Tables[data.Names[matchIndex]]
		name := data.Gensym(matchIndex)
		eachName := data.NamedGensym("each")

		return fmt.Sprintf("select %s.key from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
			eachName, baseTableName, name, name, strings.Join(f.Path, "."), eachName, name, baseTableName), nil
	}}
}

func (f FieldVal) IterableObjectGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		baseTableName := data.Tables[data.Names[matchIndex]]
		name := data.Gensym(matchIndex)
		eachName := data.NamedGensym("each")

		return fmt.Sprintf("select json_object(%s.key, %s.value) from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
			eachName, eachName, baseTableName, name, name, strings.Join(f.Path, "."), eachName, name, baseTableName), nil
	}}
}

func (j JoinFieldVal) NumericGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, _ int) (string, error) {
		data.Refs[j.Name] = true

		return fmt.Sprintf("json_extract(%s.DATA, '$.%s')", j.Name, strings.Join(j.Path, ".")), nil
	}}
}

func (j JoinFieldVal) ComparableGenerate() Instantiable {
	return j.NumericGenerate()
}

func (j JoinFieldVal) IterableValueGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		baseTableName := data.Tables[data.Names[matchIndex]]
		name := data.NamedGensym(baseTableName)
		eachName := data.NamedGensym("each")

		return fmt.Sprintf("select %s.value from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
			eachName, baseTableName, name, name, strings.Join(j.Path, "."), eachName, name, baseTableName), nil
	}}
}

func (j JoinFieldVal) IterableKeyGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		baseTableName := data.Tables[data.Names[matchIndex]]
		name := data.NamedGensym(baseTableName)
		eachName := data.NamedGensym("each")

		return fmt.Sprintf("select %s.key from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
			eachName, baseTableName, name, name, strings.Join(j.Path, "."), eachName, name, baseTableName), nil
	}}
}

func (j JoinFieldVal) IterableObjectGenerate() Instantiable {
	return Instantiable{InstFunc: func(data *InstantiationData, matchIndex int) (string, error) {
		baseTableName := data.Tables[data.Names[matchIndex]]
		name := data.NamedGensym(baseTableName)
		eachName := data.NamedGensym("each")

		return fmt.Sprintf("select json_object(%s.key, %s.value) from %s %s, json_each(%s.DATA, '$.%s') %s where %s.id = %s.id",
			eachName, eachName, baseTableName, name, name, strings.Join(j.Path, "."), eachName, name, baseTableName), nil
	}}
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
