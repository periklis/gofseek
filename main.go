package main

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	pathFlag  = "path"
	limitFlag = "limit"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&logrus.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
}

// FileDiskUsage descriptor for file disk usage
type FileDiskUsage struct {
	Path string
	Size int64
}

func initFlags(cmd string, args []string) *flag.FlagSet {
	flagset := flag.NewFlagSet(cmd, flag.ExitOnError)
	flagset.StringP(pathFlag, "p", "", "defines the target path to search for files and their disk usage")
	flagset.IntP(limitFlag, "l", 100, "defines the limit of top biggest files to print out")
	flagset.Parse(args)

	return flagset
}

func main() {
	flags := initFlags(os.Args[0], os.Args[1:])

	path, err := flags.GetString(pathFlag)
	if err != nil {
		log.Fatal(err)
	}

	if path == "" {
		log.Fatalf("argument `--%s` to seek biggest files required", pathFlag)
	}

	limit, err := flags.GetInt(limitFlag)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Seeking top %d biggest files in path '%s'", limit, path)

	files := make(chan FileDiskUsage)
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		log.Printf("Starting with path: %s", path)
		if err := walkDir(path, wg, files); err != nil {
			log.Printf("Error processing path %s: %v", path, err)
		}
	}()

	go func() {
		wg.Wait()
		close(files)
	}()

	currentStatus := make(chan []FileDiskUsage)
	go func() {
		collectLastN(files, currentStatus, limit)
	}()

	semaphore := make(chan struct{}, 1)
	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for cs := range currentStatus {
			select {
			case <-ticker.C:
				if cs != nil {
					printLastN(cs)
				}
			case _, ok := <-files:
				if !ok {
					ticker.Stop()
					printLastN(cs)
					semaphore <- struct{}{}
				}
			}
		}
	}()

	<-semaphore
	close(semaphore)
	close(currentStatus)
}

func walkDir(path string, wg *sync.WaitGroup, files chan<- FileDiskUsage) error {
	defer wg.Done()

	log.Debugf("Processing path %s\n", path)

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	dirents, err := file.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, entry := range dirents {
		entryPath := fmt.Sprintf("%s/%s", path, entry)
		fentry, err := os.Stat(entryPath)
		if err != nil {
			return err
		}

		if fentry.IsDir() {
			wg.Add(1)
			go func(subDir string, wg *sync.WaitGroup, fc chan<- FileDiskUsage) {
				walkDir(subDir, wg, fc)
			}(entryPath, wg, files)
		} else {
			files <- FileDiskUsage{
				Path: entryPath,
				Size: fentry.Size(),
			}
		}

	}

	return nil
}

func collectLastN(files <-chan FileDiskUsage, nextOut chan<- []FileDiskUsage, n int) {
	cap := n + 1
	buf := make([]FileDiskUsage, cap)

	for file := range files {
		buf = append(buf, file)

		if len(buf) < cap {
			continue
		}

		sort.Slice(buf, func(i, j int) bool {
			return buf[i].Size > buf[j].Size
		})

		buf = buf[0:n]
		nextOut <- buf
	}

	if len(buf) <= n {
		sort.Slice(buf, func(i, j int) bool {
			return buf[i].Size > buf[j].Size
		})

		nextOut <- buf
	}
}

func printLastN(out []FileDiskUsage) {
	for _, f := range out {
		fmt.Printf("Path: %s \t\t-> %d\n", f.Path, f.Size)
	}
}
