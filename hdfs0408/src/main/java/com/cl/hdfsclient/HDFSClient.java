package com.cl.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author chulang
 * @date 2020/4/8
 * @description 通过HDFS的API实现HDFS文件系统上的文件的
 * 上传、下载、重命名、删除等各种操作。（直接调用HDFS文件系统的方法，很简单）
 * <p>
 * 异常的处理原则：给别人写方法的时候抛异常（功能性代码）（不知道别人希望出现异常时怎么处理）
 * 用别人写的方法时处理异常（业务逻辑代码）
 */

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                new Configuration(), "cl");
        System.out.println("Before!!!!!!!!!!");
    }

    @Test
    /*将本地文件上传到HDFS上*/
    public void put() throws IOException, InterruptedException {

        // 获取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "cl");

        // 用这个对象操作文件系统（copyFromLocalFile方法）
        fileSystem.copyFromLocalFile(new Path("/Users/chulang/learn/hadoop/hadoop_java_API/test"), new Path("/"));

        //关闭文件系统
        fileSystem.close();

    }

    @Test
    /*使用本地配置文件的上传操作*/
    public void put1() throws IOException, InterruptedException {

        //设置配置文件
        Configuration configuration = new Configuration();
        //hadoop集群上副本数量设置为3，本地代码里配置文件默认为2，在这里手动设置为1
        configuration.setInt("dfs.replication",1);

        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"cl");

        fs.copyFromLocalFile(new Path("/Users/chulang/learn/hadoop/hadoop_java_API/test.txt"),
                new Path("/test"));

        fs.close();

    }

    @Test
    /*从HDFS上获取文件到本地*/
    public void get() throws IOException, InterruptedException {

        // 获取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                configuration, "cl");

        // 用这个对象操作文件系统（copyToLocalFile方法）
        fileSystem.copyToLocalFile(new Path("/test"), new Path("/Users/chulang/Desktop"));

        //关闭文件系统
        fileSystem.close();

    }


    @Test
    /*HDFS上文件重命名*/
    public void rename() throws IOException, InterruptedException {
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                new Configuration(), "cl");

        //操作（rename方法）
        fileSystem.rename(new Path("/test"), new Path("/test1"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    /*删除HDFS上文件*/
    public void delete() throws IOException {
        //第二个参数是是否递归删除
        boolean delete = fs.delete(new Path("/wcoutput"), true);

        if (delete) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }

    /*在文件中添加内容*/
    @Test
    public void append() throws IOException {
        // 创建本地输入流
        FileInputStream open = new FileInputStream("/Users/chulang/learn/hadoop/hadoop_java_API/test2.txt");
        // 创建HDFS系统的输出流
        FSDataOutputStream append = fs.append(new Path("/test.txt"),
                1024);
        //通过IOUtils.copyBytes完成数据传输
        IOUtils.copyBytes(open, append, 1024, true);
    }

    /*查看目录内容*/
    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("以下信息是一个文件的信息：");
                Path filePath = fileStatus.getPath();
                long modificationTime = fileStatus.getModificationTime();
                System.out.println(filePath + "--" + modificationTime);
            } else {
                System.out.println("这是一个文件夹：" );
                System.out.println(fileStatus.getPath());
            }

        }
    }

    /*利用递归将目录下的文件显示出来，不显示文件夹，（只有文件有块信息，文件夹没有（空文件也没有））*/
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()){
            LocatedFileStatus file = files.next();

            System.out.println("****************");
            System.out.println(file.getPath());

            //获取块信息(上面一个没有这个方法）
            BlockLocation[] blockLocations = file.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                System.out.println("块在：");
                for(String host : hosts){
                    System.out.println(host + "");
                }

            }

        }
    }

    @After
    public void after() throws IOException {
        System.out.println("After!!!!!!!");
        fs.close();
    }
}
