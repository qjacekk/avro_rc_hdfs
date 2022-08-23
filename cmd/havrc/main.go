package main

import (
	"github.com/qjacekk/avrc_hdfs"
	"flag"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"time"
	"github.com/colinmarc/hdfs/v2"
)

func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func main() {
	var pPattern = flag.String("p", "*.avro", "glob pattern to match file names if <path> is a directory")
	var st = flag.Bool("s", false, "use single thread")
	var v = flag.Bool("v", false, "verbose")	
	var bsize = flag.Int("b", avrc_hdfs.BuffSize, "buffer size")
	var timeout = flag.Int("t", 20, "connection timeout in seconds")
	flag.Usage = func () {
		fmt.Fprintln(flag.CommandLine.Output(), "Get Avro HDFS files statistics")
		fmt.Fprintln(flag.CommandLine.Output(), "  usage: havrc [options] <url>")
		fmt.Fprintln(flag.CommandLine.Output(), "     <url> to a single file or directory to scan recursively where")
		fmt.Fprintln(flag.CommandLine.Output(), "       <url> can be either hdfs://<namenode>:<port>/<path> or ")
		fmt.Fprintln(flag.CommandLine.Output(), "       <path> if env HADOOP_NAMENODE is set.")
		fmt.Fprintln(flag.CommandLine.Output(), "  returns: row_count total_file_size num_of_files")
		fmt.Fprintln(flag.CommandLine.Output(), "Options:")
		flag.PrintDefaults()
		fmt.Fprintln(flag.CommandLine.Output(), "Optional env vars:")
		fmt.Fprintln(flag.CommandLine.Output(), "  HADOOP_USER_NAME: HDFS user name (dafault: current user)")
		fmt.Fprintln(flag.CommandLine.Output(), "  HADOOP_NAMENODE : NameNode host and port '<name_node_host>:<port>' to use when not provided as <path>")
	}
	flag.Parse()
	if _, err := filepath.Match(*pPattern, ""); err != nil {
		log.Fatalf("Invalid pattern: %s (%v)", *pPattern, err)
	}
	if avrc_hdfs.BuffSize != *bsize {
		avrc_hdfs.BuffSize = *bsize
	}
	if *v {
		avrc_hdfs.Verbose = true
		log.Printf("Reader buffer size: %d\n", avrc_hdfs.BuffSize)
		if *st {
			log.Println("Single thread mode")
		}
		log.Printf("Glob pattern: %s\n", *pPattern)
	}
	if flag.NArg() > 0 {
		urlString := flag.Arg(0)
		options, path := avrc_hdfs.ParseHDFSPath(urlString)
		dialFunc := (&net.Dialer{
			Timeout:   time.Duration(*timeout) * time.Second,
			DualStack: true,
		}).DialContext
		options.NamenodeDialFunc = dialFunc
		options.DatanodeDialFunc = dialFunc
		
		if *v {
			log.Printf("Client opts: hosts: %v, user: %v, timeout: %v\n", options.Addresses, options.User, *timeout)
		}
		client, err := hdfs.NewClient(options)
		CheckErr(err)
		avrc_hdfs.Client = *client
		var totalRowCnt, totalSize, fileCount int64
		if *st {
			totalRowCnt, totalSize, fileCount = avrc_hdfs.ScanHdfsPath(path, *pPattern)
		} else {
			totalRowCnt, totalSize, fileCount = avrc_hdfs.ScanHdfsPathMT(path,*pPattern)
		}
		fmt.Println(totalRowCnt, totalSize, fileCount)
	} else {
		flag.Usage()
	}
}
