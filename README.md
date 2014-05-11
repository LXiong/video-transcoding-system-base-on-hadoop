video-transcoding-system-base-on-hadoop
=======================================

这是一个基于Hadoop的视频转码系统，首先视频在本地进行分割成64M左右大小，然后从本地文件夹中上传到HDFS中，然后启动一个MapReduce作业，每一个Map作业下载一个视频到本地，进行转码到在上传到HDFS中，没有reduce过程，最后客户端自行下载这些转码后的分片进行合并
