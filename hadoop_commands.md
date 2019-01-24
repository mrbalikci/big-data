- remove all files 
`hadoop fs -rm  /apps/hive/data/NY_VOLTAGE_MEASURES/dt=2018-06-05/*`

- remove all directories 
`hadoop fs -rmdir  /apps/hive/data/NY_VOLTAGE_MEASURES/*`

- list all the files in a directory 
 `hadoop fs -ls /apps/hive/data/NY_DAILY_VOLTAGE`

 - add files to a partitioning 
 `hadoop fs -put *.csv /apps/hive/data/NY_VOLTAGE_MEASURES/dt=2018-07-21`
 