#!/bin/bash
docker run -d --name namenode -p 9870:9870 -p 9000:9000 -v hadoop_namenode:/hadoop/dfs/name bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
docker run -d --name datanode -p 9864:9864 -v hadoop_datanode:/hadoop/dfs/data --link namenode:namesever bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8
