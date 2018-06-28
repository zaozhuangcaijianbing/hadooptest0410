package com.wn.hdfs;// cc FileSystemCat Displays files from a Hadoop filesystem on standard output by using the FileSystem directly

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

//读取hdfs文件内容并打印到控制台
public class FileSystemCat {

    public static void main(String[] args) throws Exception {
        String uri = "/wntest/yarn-site.xml";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://server3:9000");
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
// ^^ FileSystemCat
