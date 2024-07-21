# sparkini

## Namenode and datanodes (HDFS)
The Namenode is the master node which persist metadata in HDFS and the datanode is the slave node which store the data. When you insert data or create objects into Hive tables, data will be stored in HDFS on Hadoop DataNodes and the NameNode will keep the tracking of which DataNode has the data.

- namenode (fjardim/namenode_sqoop)
- datanode1 (fjardim/datanode)
- datanode2 (fjardim/datanode)

## Hue

Hue is an open source SQL Assistant for Databases & Data Warehouses, It is not necessary for a big data ecosystem, but it can help you visualize data in HDFS faster, and other notable features.

- namenode (fjardim/hue)
- database(fjardim/mysql)


```
shanks@pc cd sparkini/docker
docker-compose up -d
```

## Contributing and Feedback
Took the inspirations from https://github.com/fabiogjardim