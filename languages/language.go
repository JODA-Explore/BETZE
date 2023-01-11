package languages

import (
	"github.com/JODA-Explore/BETZE/languages/joda"
	"github.com/JODA-Explore/BETZE/languages/jq"
	"github.com/JODA-Explore/BETZE/languages/mongodb"
	"github.com/JODA-Explore/BETZE/languages/postgres"
	"github.com/JODA-Explore/BETZE/languages/spark"
	"github.com/JODA-Explore/BETZE/query"
)

// Language interface, specifying the methods each language module has to support.
type Language interface {
	// Returns the display name of the language, used for display purposes only
	Name() string
	// Returns the short name of the language. Has to be unique.
	ShortName() string
	// Translates a Query ito the language
	Translate(query query.Query) string
	// Writes a comment with the system specific comment syntax.
	Comment(comment string) string
	// Returns necessary header string to be added as preface to the system-specific file
	Header() string
	// Returns the delimiting symbol/string to terminate a query
	QueryDelimiter() string
	// Returns wether the language supports intermediate sets
	SupportsIntermediate() bool
}

func LanguageIndex() []interface{ Language } {
	return []interface{ Language }{
		jq.Jq{},
		mongodb.MongoDB{},
		postgres.Postgres{},
		spark.Spark{},
		joda.Joda{},
	}
}
