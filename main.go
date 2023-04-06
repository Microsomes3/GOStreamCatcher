package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"microsomes.com/gostreamcatcher/streamutil"
)

var (
	jobToProcessQueue = make(chan SteamJob, 200) // A buffered channel to act as a queue
	workers           = 2
)

type JobStatus struct {
	State  string   `json:"state"`
	Result []string `json:"result"`
}

var jobStatuses = make(map[string]JobStatus)
var jobStatusLock = sync.RWMutex{}

func QueueWork(job SteamJob) {
	jobStatusLock.Lock()
	jobStatuses[job.JobID] = JobStatus{
		State:  "queued",
		Result: []string{},
	}
	jobStatusLock.Unlock()

	jobToProcessQueue <- job
}

func worker(job <-chan SteamJob) {
	for {
		select {
		case j := <-job:
			time.Sleep(2 * time.Second)

			fmt.Println("Processed job: ", j.JobID)
			fmt.Println(j)

			jobStatusLock.Lock()
			jobStatuses[j.JobID] = JobStatus{
				State:  "recording",
				Result: []string{},
			}
			jobStatusLock.Unlock()
			data, err := streamutil.ProcessDownload(j.YoutubeLink, j.TimeoutSeconds, j.JobID, j.IsStart)

			if err != nil {
				fmt.Println("Error: ", err)
				jobStatuses[j.JobID] = JobStatus{
					State:  "error",
					Result: []string{},
				}
			} else {
				fmt.Println("Files: ", data)
				jobStatuses[j.JobID] = JobStatus{
					State:  "success",
					Result: data.Paths,
				}
			}

		default:
			// fmt.Println("No jobs to process:", time.Now().Format("2006-01-02 15:04:05"))
			time.Sleep(1 * time.Second)

		}
	}
}

func startWorkers(wg *sync.WaitGroup) {
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(jobToProcessQueue)
		}()
	}
}

type SteamJob struct {
	JobID          string `json:"jobId"`
	YoutubeLink    string `json:"youtubeLink"`
	TimeoutSeconds int    `json:"timeout"`
	IsStart        bool   `json:"isStart"`
}

func main() {

	var wg = sync.WaitGroup{}

	go startWorkers(&wg)

	http.HandleFunc("/job", func(w http.ResponseWriter, r *http.Request) {
		//only post
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := ioutil.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "Error reading body", http.StatusInternalServerError)
		}

		fmt.Println("Body: ", string(body))

		//convert to json
		var job SteamJob

		json.Unmarshal(body, &job)

		fmt.Println("Job: ", job)

		//if job exists in the status map, return the status

		if _, ok := jobStatuses[job.JobID]; ok {
			fmt.Println("Job exists in status map")
			fmt.Println(jobStatuses[job.JobID])

			toReturn, _ := json.Marshal(jobStatuses[job.JobID])

			w.Write([]byte(toReturn))
			return
		}

		QueueWork(job)

		// w.WriteHeader(http.StatusOK)

		w.Write([]byte("OK"))
	})

	//listen on port 9005

	http.ListenAndServe(":9005", nil)

}
