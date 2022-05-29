package ddbrew

func worker(id int, tasks <-chan Task, results chan<- Result) {
	for t := range tasks {
		result := t.Run()

		results <- result
	}
}
