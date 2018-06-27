package orchestrator

// workerLoad stores information used to assign tasks to workers.
type workerLoad struct {
	worker    Worker
	taskCount int
}

// counts looks at each worker and gathers the number of tasks each has.
func counts(actual, toRemove map[Worker][]interface{}) []workerLoad {
	var results []workerLoad
	for k, v := range actual {
		results = append(results, workerLoad{
			worker:    k,
			taskCount: len(v) - len(toRemove[k]),
		})
	}
	return results
}
