package query

import (
	"fmt"
	"math"
	"strings"

	"github.com/JODA-Explore/BETZE/dataset"
)

//TODO Unique and count

// A Predicate represents operations performed during the filter phase
type Predicate interface {
	// Returns the estimated selectivity of filtering the given dataset with the predicate
	Selectivity(d dataset.DataSet) float64
	// Translates the predicate to a human readable format
	String() string
}

// AndPredicate evaluates the boolean AND operation between two predicates
type AndPredicate struct {
	Lhs Predicate
	Rhs Predicate
}

func (q AndPredicate) String() string {
	return fmt.Sprintf("(%s AND %s)", q.Lhs.String(), q.Rhs.String())
}

// Selectivity implements Predicate.Selectivity by multiplying the selectivities of the sub-predicates
func (p AndPredicate) Selectivity(d dataset.DataSet) float64 {
	lhs := p.Lhs.Selectivity(d)
	rhs := p.Rhs.Selectivity(d)
	return lhs * rhs
}

// OrPredicate evaluates the boolean OR operation between two predicates
type OrPredicate struct {
	Lhs Predicate
	Rhs Predicate
}

func (q OrPredicate) String() string {
	return fmt.Sprintf("(%s OR %s)", q.Lhs.String(), q.Rhs.String())
}

// Selectivity implements Predicate.Selectivity by adding the selectivities of the sub-predicates
func (p OrPredicate) Selectivity(d dataset.DataSet) float64 {
	lhs := p.Lhs.Selectivity(d)
	rhs := p.Rhs.Selectivity(d)
	return math.Min(lhs+rhs, 1.0)
}

// Predicate evaluating the existence of the given path
type ExistsPredicate struct {
	Path string
}

func (q ExistsPredicate) String() string {
	return fmt.Sprintf("EXISTS('%s')", q.Path)
}

func (p ExistsPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil {
		return 0.0
	}

	if dataPath.Count == nil {
		return 0.5
	}

	return float64(*dataPath.Count) / float64(d.GetSize())
}

// Predicate evaluating the type of the given path
type IsStringPredicate struct {
	Path string
}

func (q IsStringPredicate) String() string {
	return fmt.Sprintf("ISSTRING('%s')", q.Path)
}

func (p IsStringPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Stringtype == nil {
		return 0.0
	}
	strType := dataPath.Stringtype
	if strType.Count != nil {
		return getTypeSelectivity(d, strType.Count)
	}

	return float64(*dataPath.Count) / float64(d.GetSize())
}

// IntEqualityPredicate evaluates the Number equality operation between a path and a given number
type IntEqualityPredicate struct {
	Path   string
	Number int64
}

func (q IntEqualityPredicate) String() string {
	return fmt.Sprintf("'%s' == %d", q.Path, q.Number)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.01 is assumed and returned (predicate selects 1% of all documents)
// If a count exists, equality assumes that exactly one element is chosen, hence a selectivity of 1/count is returned.
// If min and max exists, a uniform distribution is assumed if the value is within the bounds and a selectivity of (1/(max-min)) is returned.
func (p IntEqualityPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Inttype == nil {
		return 0.0
	}
	intType := dataPath.Inttype
	typeSelectivity := getTypeSelectivity(d, intType.Count)
	if intType.Min != nil && p.Number < *intType.Min {
		return 0.0
	}
	if intType.Max != nil && p.Number > *intType.Max {
		return 0.0
	}
	if intType.Count == nil {
		return 0.01 * typeSelectivity
	}
	if intType.Min != nil && intType.Max != nil {
		return (1.0 / float64((*intType.Max-*intType.Min)+1)) * typeSelectivity
	}
	return (1.0 / float64(*intType.Count)) * typeSelectivity
}

// FloatComparisonPredicate evaluates the Number comparison (<,>,<=,>=) operation between a path and a given number
type FloatComparisonPredicate struct {
	Path    string
	Number  float64
	Smaller bool
	Equal   bool
}

func (q FloatComparisonPredicate) String() string {
	var cmpstr = ">"
	if q.Smaller {
		cmpstr = "<"
	}
	if q.Equal {
		cmpstr += "="
	}
	return fmt.Sprintf("'%s' %s %f", q.Path, cmpstr, q.Number)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.33333 is assumed and returned (predicate selects 1/3 of all documents)
// If min and max exists, a uniform distribution is assumed if the value is within the bounds and a selectivity of (1/(max-min)) is returned.
func (p FloatComparisonPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Floattype == nil {
		return 0.0
	}
	floatType := dataPath.Floattype
	intType := dataPath.Inttype
	typeSelectivity := getTypeSelectivity(d, floatType.Count) + getTypeSelectivity(d, intType.Count)
	if floatType.Min != nil && p.Number < *floatType.Min {
		if p.Smaller {
			return 0.0
		}
		return 1.0 * typeSelectivity
	}
	if floatType.Max != nil && p.Number > *floatType.Max {
		if !p.Smaller {
			return 0.0
		}
		return 1.0 * typeSelectivity

	}
	if floatType.Min != nil && floatType.Max != nil {
		// TODO Equal?
		abs := (float64(p.Number-*floatType.Min) + 1.0) / (float64(*floatType.Max-*floatType.Min) + 1.0)
		if p.Smaller {
			return abs
		}
		return (1.0 - abs) * typeSelectivity
	}
	return (1.0 / 3.0) * typeSelectivity
}

// StrEqualityPredicate evaluates the String equality operation between a path and a given string
type StrEqualityPredicate struct {
	Path string
	Str  string
}

func (q StrEqualityPredicate) String() string {
	return fmt.Sprintf("'%s' == \"%s\"", q.Path, q.Str)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.01 is assumed and returned (predicate selects 1% of all documents)
// If a count exists, equality assumes that exactly one element is chosen, hence a selectivity of 1/count is returned.
// If min and max exists, a uniform distribution is assumed if the value is within the bounds and a selectivity of (1/(max-min)) is returned.
func (p StrEqualityPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Stringtype == nil {
		return 0.0
	}
	strType := dataPath.Stringtype
	typeSelectivity := getTypeSelectivity(d, strType.Count)
	if strType.Min != nil && p.Str < *strType.Min {
		return 0.0
	}
	if strType.Max != nil && p.Str > *strType.Max {
		return 0.0
	}
	if strType.Count == nil {
		return 0.01 * typeSelectivity
	}
	return (1.0 / float64(*strType.Count)) * typeSelectivity
}

// StrPrefixPredicate checks if a given path contains a string with the given prefix
type StrPrefixPredicate struct {
	Path   string
	Prefix string
}

func (q StrPrefixPredicate) String() string {
	return fmt.Sprintf("HAS_PREFIX('%s',\"%s\")", q.Path, q.Prefix)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.01 is assumed and returned (predicate selects 1% of all documents)
// If a count and prefix list exists with a matching prefix, uniform distribution is assumed and 1/#prefixes is returned
func (p StrPrefixPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Stringtype == nil {
		return 0.0
	}
	strType := dataPath.Stringtype
	typeSelectivity := getTypeSelectivity(d, strType.Count)
	if strType.Count == nil {
		return 0.01 * typeSelectivity
	}

	if strType.Prefixes != nil && len(strType.Prefixes) > 0 {
		contains := false
		for _, prefix := range strType.Prefixes {
			if strings.HasPrefix(prefix, p.Prefix) {
				contains = true
				break
			}
		}
		if !contains {
			return 0.0
		}
		return (1.0 / float64(len(strType.Prefixes))) * typeSelectivity
	}

	return (1.0 / float64(*strType.Count)) * typeSelectivity
}

// BoolEqualityPredicate evaluates the boolean equality operation between a path and a boolean
type BoolEqualityPredicate struct {
	Path  string
	Value bool
}

func (q BoolEqualityPredicate) String() string {
	return fmt.Sprintf("'%s' == %t", q.Path, q.Value)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.5 is assumed and returned (predicate selects 50% of all documents)
// If true/false counts exist, an exact selectivity is returned.
func (p BoolEqualityPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Booltype == nil {
		return 0.0
	}
	boolType := dataPath.Booltype
	typeSelectivity := getTypeSelectivity(d, boolType.Count)
	if typeSelectivity == 0 {
		return 0.0
	}

	if boolType.Count != nil && boolType.TrueCount != nil && p.Value { // Value is true and we have a true-count
		return (float64(*boolType.TrueCount) / float64(*boolType.Count)) * typeSelectivity
	}

	if boolType.Count != nil && boolType.FalseCount != nil && !p.Value { // Value is false and we have a false-count
		return (float64(*boolType.FalseCount) / float64(*boolType.Count)) * typeSelectivity
	}

	return 0.5 * typeSelectivity
}

func getTypeSelectivity(dataset dataset.DataSet, typeCount *uint64) float64 {
	if typeCount != nil && *typeCount == 0 { // Type does not exist
		return 0.0
	}
	if typeCount != nil && dataset.Count != nil { //We know both counts, calculate type selectivity
		return float64(*typeCount) / float64(*dataset.Count)
	}
	return 0.33 //We do not know how selective the type is, estimate 0.33
}

// ObjectSizeComparisonPredicate evaluates the Number comparison (<,>,<=,>=) operation between the number of members in a path and a given number
type ObjectSizeComparisonPredicate struct {
	Path    string
	Number  uint64
	Smaller bool
	Equal   bool
}

func (q ObjectSizeComparisonPredicate) String() string {
	var cmpstr = ">"
	if q.Smaller {
		cmpstr = "<"
	}
	if q.Equal {
		cmpstr += "="
	}
	return fmt.Sprintf("MEMBERCOUNT('%s') %s %d", q.Path, cmpstr, q.Number)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.33333 is assumed and returned (predicate selects 1/3 of all documents)
// If min and max exists, a uniform distribution is assumed if the value is within the bounds and a selectivity of (1/(max-min)) is returned.
func (p ObjectSizeComparisonPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Objecttype == nil {
		return 0.0
	}
	objectType := dataPath.Objecttype
	typeSelectivity := getTypeSelectivity(d, objectType.Count)
	if objectType.MinMembers != nil && p.Number < *objectType.MinMembers {
		if p.Smaller {
			return 0.0
		}
		return 1.0 * typeSelectivity
	}
	if objectType.MaxMembers != nil && p.Number > *objectType.MaxMembers {
		if !p.Smaller {
			return 0.0
		}
		return 1.0 * typeSelectivity

	}
	if objectType.MinMembers != nil && objectType.MaxMembers != nil {
		// TODO Equal?
		abs := (float64(p.Number-*objectType.MinMembers) + 1.0) / (float64(*objectType.MaxMembers-*objectType.MinMembers) + 1.0)
		if p.Smaller {
			return abs * typeSelectivity
		}
		return (1.0 - abs) * typeSelectivity
	}
	return (1.0 / 3.0) * typeSelectivity
}

// ArraySizeComparisonPredicate evaluates the Number comparison (<,>,<=,>=) operation between the number of entries in an array path and a given number
type ArraySizeComparisonPredicate struct {
	Path    string
	Number  uint64
	Smaller bool
	Equal   bool
}

func (q ArraySizeComparisonPredicate) String() string {
	var cmpstr = ">"
	if q.Smaller {
		cmpstr = "<"
	}
	if q.Equal {
		cmpstr += "="
	}
	return fmt.Sprintf("SIZE('%s') %s %d", q.Path, cmpstr, q.Number)
}

// Selectivity implements Predicate.Selectivity by estimating the selectivity given the data set.
// If no DataPath with matching type and path exists, 0 is returned
// If a data path exists, but has no count, 0.33333 is assumed and returned (predicate selects 1/3 of all documents)
// If min and max exists, a uniform distribution is assumed if the value is within the bounds and a selectivity of (1/(max-min)) is returned.
func (p ArraySizeComparisonPredicate) Selectivity(d dataset.DataSet) float64 {
	dataPath := d.Paths[p.Path]
	if dataPath == nil || dataPath.Arraytype == nil {
		return 0.0
	}
	arrayType := dataPath.Arraytype
	typeSelectivity := getTypeSelectivity(d, arrayType.Count)
	if arrayType.MinSize != nil && p.Number < *arrayType.MinSize {
		if p.Smaller {
			return 0.0
		}
		return 1.0 * typeSelectivity
	}
	if arrayType.MaxSize != nil && p.Number > *arrayType.MaxSize {
		if !p.Smaller {
			return 0.0
		}
		return 1.0 * typeSelectivity

	}
	if arrayType.MinSize != nil && arrayType.MaxSize != nil {
		// TODO Equal?
		abs := (float64(p.Number-*arrayType.MinSize) + 1.0) / (float64(*arrayType.MaxSize-*arrayType.MinSize) + 1.0)
		if p.Smaller {
			return abs * typeSelectivity
		}
		return (1.0 - abs) * typeSelectivity
	}
	return (1.0 / 3.0) * typeSelectivity
}
