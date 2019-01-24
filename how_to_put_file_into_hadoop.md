## How to load files into Hortonworks Sandbox 

- For this particular example, I am using `Bitvise SSH` client. 
- maria_dev is one one the default client and the password is `maria_dev` also. 

## Steps
- in `maria-dev`
`Hadoop fs -ls /` will give you the file system in hdfs and Hadoop in sandbox

- in this example, we will use tmp
`Hadoop fs -ls /tmp`

- check the file you need in the db 
`hadoop fs -ls /tmp/<the db name>`

- `ls` to see what files are ready to be moved to hdfs 

- move the file 
`hadoop fs -put <file name> /<directory>`

- move multiple files into hdfs(hadoop file system) from vertiual envoirement 
`hadoop fs -put *.csv /apps/hive/data/NY_NOTIFICATION`

- check if the file is in hdfs 
`hadoop fs -ls /<directory>`

- After completing this step, go to Ambari > File View and verify if the files are in `tmp`