package com.liang.spark.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class SparkTest2 {
    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path("obs://hadoop-obs/flink/");
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(path, true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            Path path1 = locatedFileStatusRemoteIterator.next().getPath();
            System.out.println(path1);
            //FSDataInputStream stream = fileSystem.open(path1);
            //LineIter iter = IoUtil.lineIter(stream, StandardCharsets.UTF_8);
            //while (iter.hasNext()) {
            //    System.out.println(iter.next());
            //}
        }
    }
}
