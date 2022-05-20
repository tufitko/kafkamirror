package labels

import "os"

// Build info provided by linker at build time
var (
	GitBranch   string
	GitCommit   string
	BuildNumber string
	BuildDate   string
)

var Labels map[string]string

func init() {
	if GitBranch == "" {
		GitBranch, _ = os.LookupEnv("GIT_BRANCH")
		if GitBranch == "" {
			GitBranch = "unknown"
		}
	}

	if GitCommit == "" {
		GitCommit, _ = os.LookupEnv("GIT_COMMIT")
		if GitCommit == "" {
			GitCommit = "unknown"
		}
	}

	if BuildNumber == "" {
		BuildNumber, _ = os.LookupEnv("BUILD_NUMBER")
		if BuildNumber == "" {
			BuildNumber = "1"
		}
	}

	if BuildDate == "" {
		BuildDate, _ = os.LookupEnv("BUILD_DATE")
		if BuildDate == "" {
			BuildDate = "2021-08-05T07:22:13Z"
		}
	}

	Labels = map[string]string{
		"git_branch":   GitBranch,
		"git_commit":   GitCommit,
		"build_number": BuildNumber,
		"build_date":   BuildDate,
	}
}

func Add(labels map[string]string) {
	for k, v := range labels {
		Labels[k] = v
	}
}

func GetLoggerLabels() map[string]interface{} {
	converted := make(map[string]interface{})
	for k, v := range Labels {
		converted[k] = v
	}
	return converted
}
