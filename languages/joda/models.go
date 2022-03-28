package joda

// Response to the /system endpoint of the JODA server
type System struct {
	Memory  SystemMemory  `json:"memory"`
	Version SystemVersion `json:"version"`
	Host    SystemHost    `json:"host"`
}
type SystemMemory struct {
	Total            int64 `json:"total"`
	Used             int64 `json:"used"`
	Joda             int   `json:"joda"`
	AllowedMemory    int64 `json:"allowed_memory"`
	CalculatedMemory int   `json:"calculated_memory"`
}
type SystemVersion struct {
	Version   string `json:"version"`
	API       int    `json:"api"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build-time"`
}
type SystemHost struct {
	Kernel string `json:"kernel"`
	Os     string `json:"os"`
}

// Response to the /sources endpoint of the JODA server
type Sources []struct {
	Name      string `json:"name"`
	Documents int    `json:"documents"`
	Container int    `json:"container"`
	Memory    int    `json:"memory"`
	MemoryStr string `json:"memory-str"`
}

// Response to the /query endpoint of the JODA server
type Query struct {
	Success   int       `json:"success"`
	Size      int       `json:"size"`
	Benchmark Benchmark `json:"benchmark"`
	Error     string    `json:"error"`
}
type Threads struct {
	Aggregate float64 `json:"Aggregate,omitempty"`
}
type Runtime struct {
	Threads    []Threads `json:"Threads"`
	Aggmerge   float64   `json:"AggMerge"`
	Store      float64   `json:"Store"`
	Evaluation float64   `json:"Evaluation"`
	Query      float64   `json:"Query"`
}
type BenchmarkSystem struct {
	Version   string `json:"Version"`
	Build     string `json:"Build"`
	BuildTime string `json:"Build Time"`
}
type Benchmark struct {
	Query                      string          `json:"Query"`
	Time                       int             `json:"Time"`
	PrettyTime                 string          `json:"Pretty Time"`
	Threads                    int             `json:"Threads"`
	Runtime                    Runtime         `json:"Runtime"`
	ResultSize                 int             `json:"Result Size"`
	Container                  int             `json:"#Container"`
	RAMProc                    int             `json:"RAM Proc"`
	PrettyRAMProc              string          `json:"Pretty RAM Proc"`
	EstimatedStorageSize       int             `json:"Estimated Storage Size"`
	PrettyEstimatedStorageSize string          `json:"Pretty Estimated Storage Size"`
	System                     BenchmarkSystem `json:"System"`
}

// Response to the /result endpoint of the JODA server
// Each row is a row of the JSOn result of the query.
// Hence the structure of the rows depends on the query.
type Result struct {
	Result []interface{} `json:"result"`
}

// Analyze result
type Analyze struct {
	Children     []Analyze `json:"Children"`
	CountTotal   uint64    `json:"Count_Total"`
	CountObject  uint64    `json:"Count_Object"`
	MinMember    *uint64   `json:"Min_Member"`
	MaxMember    *uint64   `json:"Max_Member"`
	CountArray   uint64    `json:"Count_Array"`
	MinSize      *uint64   `json:"Min_Size"`
	MaxSize      *uint64   `json:"Max_Size"`
	CountNull    uint64    `json:"Count_Null"`
	CountBoolean uint64    `json:"Count_Boolean"`
	CountTrue    uint64    `json:"Count_True"`
	CountFalse   uint64    `json:"Count_False"`
	CountString  uint64    `json:"Count_String"`
	CountInt     uint64    `json:"Count_Int"`
	MinInt       *int64    `json:"Min_Int"`
	MaxInt       *int64    `json:"Max_Int"`
	CountFloat   uint64    `json:"Count_Float"`
	MinFloat     *float64  `json:"Min_Float"`
	MaxFloat     *float64  `json:"Max_Float"`
	CountNumber  uint64    `json:"Count_Number"`
	Key          string    `json:"Key"`
}
