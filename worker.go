package ddbrew

type Task interface {
	Run() Result
}

type Result interface {
	Count() int
	Error() error
}

func worker(id int, tasks <-chan Task, results chan<- Result) {
	for t := range tasks {
		result := t.Run()

		results <- result
	}
}
