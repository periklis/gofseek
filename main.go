package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	pathFlag  = "path"
	limitFlag = "limit"
	liveFlag  = "live"
)

var tw *tabwriter.Writer

func init() {
	tw = new(tabwriter.Writer)
	tw.Init(os.Stdout, 5, 0, 1, ' ', 0)

	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&logrus.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
}

// FileDiskUsage descriptor for file disk usage per path
type FileDiskUsage struct {
	Path string
	Size int64
}

func initFlags(cmd string, args []string) *flag.FlagSet {
	flagset := flag.NewFlagSet(cmd, flag.ExitOnError)
	flagset.StringP(pathFlag, "p", "", "Defines the target path to search for files and their disk usage")
	flagset.IntP(limitFlag, "l", 100, "Defines the limit of top biggest files to print out")
	flagset.Bool(liveFlag, false, "Enable live output")
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

	live, err := flags.GetBool(liveFlag)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Seeking top %d biggest files in path '%s'", limit, path)

	diskUsage := make(chan FileDiskUsage)
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		log.Printf("Starting with path: %s", path)
		if err := walkDir(path, wg, diskUsage); err != nil {
			log.Printf("Error processing path %s: %v", path, err)
		}
	}()

	go func() {
		wg.Wait()
		close(diskUsage)
	}()

	lastDiskUsage := make(chan []FileDiskUsage)
	go func() {
		collectLastN(diskUsage, lastDiskUsage, limit)
	}()

	drained := make(chan struct{}, 1)
	ticker := time.NewTicker(10 * time.Millisecond)

	go func() {
		var last []FileDiskUsage
		for cs := range lastDiskUsage {
			select {
			case <-ticker.C:
				if live {
					last = cs
					tw.Flush()
					printLastN(tw, last)
				}
			default:
				last = cs
			}
		}

		ticker.Stop()
		tw.Flush()
		printLastN(tw, last)
		drained <- struct{}{}
	}()

	<-drained
	close(drained)
	close(lastDiskUsage)
}

func walkDir(path string, wg *sync.WaitGroup, diskUsage chan<- FileDiskUsage) error {
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
		filePath := fmt.Sprintf("%s/%s", path, entry)
		fileinfo, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		if fileinfo.IsDir() {
			wg.Add(1)
			go func(subDir string, wg *sync.WaitGroup, fc chan<- FileDiskUsage) {
				walkDir(subDir, wg, fc)
			}(filePath, wg, diskUsage)
		} else {
			diskUsage <- FileDiskUsage{
				Path: filePath,
				Size: fileinfo.Size(),
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

func printLastN(writer io.Writer, usages []FileDiskUsage) {
	fmt.Fprint(writer, "Path\tSize\n")
	for _, f := range usages {
		fmt.Fprintf(writer, "%s\t%d\n", f.Path, f.Size)
	}
}
