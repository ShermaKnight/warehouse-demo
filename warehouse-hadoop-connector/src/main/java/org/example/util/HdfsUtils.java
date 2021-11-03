package org.example.util;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

public class HdfsUtils {

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        Properties properties = new Properties();
        InputStream in = HdfsUtils.class.getClassLoader().getResourceAsStream("application.properties");
        properties.load(in);
        String hdfs = "hdfs://" + properties.getProperty("hadoop.server");
        String destination = "/VmT/sources.list";
        put(hdfs, "D:/VmT/sources.list", destination);
        get(hdfs, "D:/sources.list", destination);
    }

    @SneakyThrows
    public static void put(String hdfs, String input, String destination) {
        File file = new File(input);
        if (!file.exists()) {
            throw new RuntimeException("file not found!");
        }

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfs);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(new Path(destination))) {
            fileSystem.delete(new Path(destination), true);
        }
        FSDataOutputStream outputStream = fileSystem.create(new Path(destination));
        IOUtils.copyBytes(new FileInputStream(file), outputStream, 1024, true);
    }

    @SneakyThrows
    public static void get(String hdfs, String output, String destination) {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfs);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (!fileSystem.exists(new Path(destination))) {
            throw new RuntimeException("file not found!");
        }

        File file = new File(output);
        if (file.exists() && file.isFile()) {
            throw new RuntimeException("output is not empty.");
        }
        FSDataInputStream inputStream = fileSystem.open(new Path(destination));
        IOUtils.copyBytes(inputStream, new FileOutputStream(file), 1024, true);
    }
}
