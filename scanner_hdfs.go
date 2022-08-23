package avrc_hdfs

import (
	"github.com/qjacekk/avro_rc"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"github.com/colinmarc/hdfs/v2"
)
func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
var Client hdfs.Client
var BuffSize = 32*1024
var Verbose = false

func ScanAvroHdfsFile(fileName string, readerBuffSize int) (int64, error) {
	f, err := Client.Open(fileName)
	CheckErr(err)
	defer f.Close()
	if Verbose {
		log.Printf("Scanning: %s\n", fileName)
	}
	rc,err:= avro_rc.ScanAvro(f, readerBuffSize)
	if Verbose {
		log.Printf("Finished scanning: %s\n", fileName)
	}
	return rc,err
}

func ScanHdfsPath(path string, pPattern string) (int64, int64, int64) {
	fi, err := Client.Stat(path)
	CheckErr(err)
	if fi.Mode().IsRegular() {
		rc, err := ScanAvroHdfsFile(path, BuffSize)
		CheckErr(err)
		return rc, fi.Size(), 1
	} else if fi.IsDir() {
		var totalRowCnt int64
		var totalSize int64
		var fileCount int64
		Client.Walk(path, func(path string, info fs.FileInfo, err error) error {
			CheckErr(err)
			if info.Mode().IsRegular() {
				if match, _ := filepath.Match(pPattern, info.Name()); match {
					rc, err := ScanAvroHdfsFile(path, BuffSize)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Unable to read file: %s, error: %s\n", path, err.Error())
						return nil
					}
					totalRowCnt += rc
					totalSize += info.Size()
					fileCount++
				}
			}
			return nil
		})
		return totalRowCnt, totalSize, fileCount
	}
	return 0,0,0
}

// Concurrent version
var wg sync.WaitGroup // waits for all workers to complete
var totalRowCnt int64
var totalSize int64
var fileCount int64

type AvroFile struct {
	path string
	size int64
}

func hdfsWorker(files <-chan *AvroFile) {
	for file := range files {
		rc, err := ScanAvroHdfsFile(file.path, BuffSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to read file: %s, error: %s\n", file.path, err.Error())
		} else {
			atomic.AddInt64(&totalRowCnt,rc)
			atomic.AddInt64(&totalSize, file.size)
			atomic.AddInt64(&fileCount, 1)
		}
	}
	wg.Done()
}
var files chan *AvroFile
// Scans file or directory using multiple threads
func ScanHdfsPathMT(path string, pPattern string) (int64, int64, int64) {
	fi, err := Client.Stat(path)
	CheckErr(err)
	if fi.Mode().IsRegular() {
		rc, err := ScanAvroHdfsFile(path, BuffSize)
		CheckErr(err)
		return rc, fi.Size(), 1
	} else if !fi.IsDir() {
		log.Fatal("input must be a file or directory")
	}
	files = make(chan *AvroFile)
	nWorkers := runtime.NumCPU()
	if Verbose {
		log.Printf("Starting %d workers\n", nWorkers)
	}
	for w := 1; w <= nWorkers; w++ {
        wg.Add(1)
        go hdfsWorker(files)
    }
	Client.Walk(path, func(path string, info fs.FileInfo, err error) error {
		CheckErr(err)
		if info.Mode().IsRegular() {
			if match, _ := filepath.Match(pPattern, info.Name()); match {
				files <- &AvroFile{path:path, size:info.Size()}
			}
		}
		return nil
	})
	close(files)
	wg.Wait()
	return totalRowCnt, totalSize, fileCount
}

func ParseHDFSPath(urlString string) (hdfs.ClientOptions, string) {
	// try to parse the url first
	url, err := url.Parse(urlString)
	CheckErr(err)
	clientOptions := hdfs.ClientOptions{}
	if url.Path != "" {
		var host string
		if strings.ToLower(url.Scheme) == "hdfs" && url.Host != "" {
			host = url.Host
		} else {  // try env var
			host = os.Getenv("HADOOP_NAMENODE")
		}
		if host != "" {
			clientOptions.Addresses = []string{host}
			if url.User.Username() != "" {
				clientOptions.User = url.User.Username()
			} else if os.Getenv("HADOOP_USER_NAME") != "" {
				clientOptions.User = os.Getenv("HADOOP_USER_NAME")
			} else {
				u, err := user.Current()
				if err == nil {
					clientOptions.User = u.Username
				}
			}
			return clientOptions, url.Path
		}
	}
	log.Fatal("use: hdfs://<name_node_host>:<port>/<path> or set HADOOP_NAMENODE to '<name_node_host>:<port>'")
	return clientOptions,""
}
