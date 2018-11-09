package main

import (
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	pathFlag  = "path"
	limitFlag = "limit"
)

var log = &logrus.Logger{
	Out:       os.Stderr,
	Formatter: &logrus.TextFormatter{},
	Hooks:     make(logrus.LevelHooks),
	Level:     logrus.InfoLevel,
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

	go func() {
		seek(path, files)
	}()

	for f := range files {
		log.Printf("%s: %d", f.Path, f.Size)
	}

}

func seek(path string, out chan<- FileDiskUsage) {
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			out <- FileDiskUsage{
				Path: path,
				Size: info.Size(),
			}
		}

		return nil
	})
	close(out)
}
