# avro_rc_hdfs

Avro_rc for HDFS

## Build & usage

```commandline
$ cd cmd/havrc

$ go build

$ ./havrc -h

Get Avro HDFS files statistics
  usage: havrc [options] <url>
     <url> to a single file or directory to scan recursively, where
       <url> can be either hdfs://<namenode>:<port>/<path> or 
       <path> if env HADOOP_NAMENODE is set.
  returns: row_count total_file_size num_of_files
Options:
  -b int
        buffer size (default 32768)
  -p string
        glob pattern to match file names if <path> is a directory (default "*.avro")
  -s    use single thread
  -t int
        connection timeout in seconds (default 20)
  -v    verbose
Optional env vars:
  HADOOP_USER_NAME: HDFS user name (dafault: current user)
  HADOOP_NAMENODE : NameNode host and port '<name_node_host>:<port>' to use when not provided as <path>
```

## Acknowledgements

This tool uses:

https://github.com/hamba/avro

https://github.com/colinmarc/hdfs
