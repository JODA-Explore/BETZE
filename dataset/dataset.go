package dataset

import (
	"math"

	"github.com/adam-lavrik/go-imath/i64"
	"github.com/adam-lavrik/go-imath/u64"
)

// DataSet represents a set of documents from the source query system
// It serves as an abstractions of the DBMS/Data Processor/... specific table/dataset/collection/...
type DataSet struct {
	// The Name/ID of the dataset
	Name string
	// The real number of documents in the dataset. Should be set if known
	Count *uint64
	// The expected number of documents
	ExpectedCount uint64
	// The merged pathinformation of all documents in the dataset
	Paths map[string]*DataPath
	// The parent DataSet, if it exists
	DerivedFrom *DataSet
}

// GetSize returns the expected or actual number of documents in the dataset
func (d *DataSet) GetSize() uint64 {
	if d.Count != nil {
		return *d.Count
	}
	return d.ExpectedCount
}

// A DataPath represents a single path within a document of a DataSet.
type DataPath struct {
	// The Path expression of the Path
	Path string
	// Information about the existence and distribution of string values
	Stringtype *StringType
	// Information about the existence and distribution of float values
	Floattype *FloatType
	// Information about the existence and distribution of integer values
	Inttype *IntType
	// Information about the existence and distribution of bool values
	Booltype *BooleanType
	// Information about the existence and distribution of null values
	Nulltype *NullType
	// Information about the existence and distribution of object values
	Objecttype *ObjectType
	// Information about the existence and distribution of array values
	Arraytype *ArrayType
	// Information about the existence of the path
	Count *uint64
}

// Merge merges two DataPaths and accumulates their statistics.
// They have to represent the same path. If not, nil is returned.
func (l *DataPath) Merge(r DataPath) *DataPath {
	if l.Path != r.Path {
		return nil
	}

	if l.Stringtype == nil {
		l.Stringtype = r.Stringtype
	} else if r.Stringtype != nil {
		l.Stringtype.merge(*r.Stringtype)
	}

	if l.Floattype == nil {
		l.Floattype = r.Floattype
	} else if r.Floattype != nil {
		l.Floattype.merge(*r.Floattype)
	}

	if l.Booltype == nil {
		l.Booltype = r.Booltype
	} else if r.Booltype != nil {
		l.Booltype.merge(*r.Booltype)
	}

	if l.Nulltype == nil {
		l.Nulltype = r.Nulltype
	} else if r.Nulltype != nil {
		l.Nulltype.merge(*r.Nulltype)
	}
	return l
}

func (l *DataPath) HasFloatCount() bool {
	return l.Floattype != nil && l.Floattype.Count != nil && *l.Floattype.Count > 0
}

func (l *DataPath) HasIntCount() bool {
	return l.Inttype != nil && l.Inttype.Count != nil && *l.Inttype.Count > 0
}

func (l *DataPath) HasNumCount() bool {
	return l.HasFloatCount() || l.HasIntCount()
}

func (l *DataPath) HasStringCount() bool {
	return l.Stringtype != nil && l.Stringtype.Count != nil && *l.Stringtype.Count > 0
}

func (l *DataPath) HasBoolCount() bool {
	return l.Booltype != nil && ((l.Booltype.Count != nil && *l.Booltype.Count > 0) || (l.Booltype.FalseCount != nil && *l.Booltype.FalseCount > 0) || (l.Booltype.TrueCount != nil && *l.Booltype.TrueCount > 0))
}

// StringType represents a possible String type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type StringType struct {
	// An optional count of how many documents have the given type at the path
	Count    *uint64
	Min      *string
	Max      *string
	Unique   *uint64
	Prefixes []string
}

func (l *StringType) merge(r StringType) *StringType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}

	if l.Unique == nil {
		l.Unique = r.Unique
	} else if r.Unique != nil {
		*l.Unique += *r.Unique
	}

	if l.Min == nil {
		l.Min = r.Min
	} else if r.Min != nil {
		if *r.Min < *l.Min {
			l.Min = r.Min
		}
	}

	if l.Max == nil {
		l.Max = r.Max
	} else if r.Max != nil {
		if *r.Max > *l.Max {
			l.Max = r.Max
		}
	}

	return l
}

// FloatType represents a possible Float type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type FloatType struct {
	// An optional count of how many documents have the given type at the path
	Count  *uint64
	Min    *float64
	Max    *float64
	Unique *uint64
}

func (l *FloatType) merge(r FloatType) *FloatType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}

	if l.Unique == nil {
		l.Unique = r.Unique
	} else if r.Unique != nil {
		*l.Unique += *r.Unique
	}

	if l.Min == nil {
		l.Min = r.Min
	} else if r.Min != nil {
		*l.Min = math.Min(*l.Min, *r.Min)
	}

	if l.Max == nil {
		l.Max = r.Max
	} else if r.Max != nil {
		*l.Max = math.Max(*l.Max, *r.Max)
	}

	return l
}

// IntType represents a possible integer type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type IntType struct {
	// An optional count of how many documents have the given type at the path
	Count  *uint64
	Min    *int64
	Max    *int64
	Unique *uint64
}

func (l *IntType) merge(r IntType) *IntType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}

	if l.Unique == nil {
		l.Unique = r.Unique
	} else if r.Unique != nil {
		*l.Unique += *r.Unique
	}

	if l.Min == nil {
		l.Min = r.Min
	} else if r.Min != nil {
		*l.Min = i64.Min(*l.Min, *r.Min)
	}

	if l.Max == nil {
		l.Max = r.Max
	} else if r.Max != nil {
		*l.Max = i64.Max(*l.Max, *r.Max)
	}

	return l
}

// BooleanType represents a possible Boolean type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type BooleanType struct {
	// An optional count of how many documents have the given type at the path
	Count      *uint64
	FalseCount *uint64
	TrueCount  *uint64
}

func (l *BooleanType) merge(r BooleanType) *BooleanType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}

	if l.FalseCount == nil {
		l.FalseCount = r.FalseCount
	} else if r.FalseCount != nil {
		*l.FalseCount += *r.FalseCount
	}

	if l.TrueCount == nil {
		l.TrueCount = r.TrueCount
	} else if r.TrueCount != nil {
		*l.TrueCount += *r.TrueCount
	}

	return l
}

// NullType represents a possible Null type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type NullType struct {
	// An optional count of how many documents have the given type at the path
	Count *uint64
}

func (l *NullType) merge(r NullType) *NullType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}
	return l
}

// ObjectType represents a possible object type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type ObjectType struct {
	// An optional count of how many documents have the given type at the path
	Count      *uint64
	MinMembers *uint64
	MaxMembers *uint64
}

func (l *ObjectType) merge(r ObjectType) *ObjectType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}

	if l.MinMembers == nil {
		l.MinMembers = r.MinMembers
	} else if r.MinMembers != nil {
		*l.MinMembers = u64.Min(*l.MinMembers, *r.MinMembers)
	}

	if l.MaxMembers == nil {
		l.MaxMembers = r.MaxMembers
	} else if r.MaxMembers != nil {
		*l.MaxMembers = u64.Max(*l.MaxMembers, *r.MaxMembers)
	}

	return l
}

// ArrayType represents a possible Array type of a DataPath.
// The type may be augmented with statistics about the distribution of the data
type ArrayType struct {
	// An optional count of how many documents have the given type at the path
	Count   *uint64
	MinSize *uint64
	MaxSize *uint64
}

func (l *ArrayType) merge(r ArrayType) *ArrayType {
	if l.Count == nil {
		l.Count = r.Count
	} else if r.Count != nil {
		*l.Count += *r.Count
	}

	if l.MinSize == nil {
		l.MinSize = r.MinSize
	} else if r.MinSize != nil {
		*l.MinSize = u64.Min(*l.MinSize, *r.MinSize)
	}

	if l.MaxSize == nil {
		l.MaxSize = r.MaxSize
	} else if r.MaxSize != nil {
		*l.MaxSize = u64.Max(*l.MaxSize, *r.MaxSize)
	}

	return l
}
