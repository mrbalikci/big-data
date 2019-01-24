
- add multiple files to the hadoop 

`hadoop fs -put flat_files/*.xml /tmp/something_test_load/flat_files`

- to see what files are in hadoop cluster in hdfs -- sandbox
`hdfs dfs -ls /tmp/something_test_load/flat_files/*.dat`

- grep files and filter by a specific 

`grep D~ flat_files/cc_dg_20180601.dat >> flat_files_clean/cc_dg_20180601.dat`

- multiple files filter in linux and save as under a different extention if needed
`for file in *.dat; do grep D~ "$file" > "$file.2"; done`

- move the files into another folder
` mv *.dat.2 ../flat_files_clean`

- and put them in hdfs (hadoop) -- sandbox
`hadoop fs -put *.dat.2 /tmp/something_test_load/flat_files_clean`
