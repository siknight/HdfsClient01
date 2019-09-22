package com.lisi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
        @Test
        public void testMkdirs() throws IOException {
            //创建配置文件
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://hadoop102:9000");
            //conf.set("","");
            //获取hdfs客户端
            FileSystem fs = FileSystem.get(conf);
            //创建文件
            fs.mkdirs(new Path("/a01"));

            //关闭资源
            fs.close();


        }

        @Test
        public void test_mkdir2() throws URISyntaxException, IOException, InterruptedException {

            //配置
            Configuration conf = new Configuration();
            //获取hdfs客户端
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lisi");
            //客户端创建文件
            fs.mkdirs(new Path("/a05"));
            //关闭文件系统
            fs.close();
            System.out.println("over");

        }

        @Test
        public void uploadFile() throws URISyntaxException, IOException, InterruptedException {

            Configuration conf = new Configuration();
            conf.set("dfs.replication","2");
            FileSystem fs
                    = FileSystem.get(new URI("hdfs://hadoop102:9000"),
                  conf, "lisi");
            fs.copyFromLocalFile(new Path("d://aaa.txt"),new Path("/a02/ccc.txt"));
            fs.close();
            System.out.println("over");
        }
        @Test
        public void deleteFile() throws URISyntaxException, IOException, InterruptedException {

            FileSystem fs = FileSystem.get(
                    new URI("hdfs://hadoop102:9000"),
                    new Configuration(),
                    "lisi");
            fs.delete(new Path("/a03"));
            fs.close();
            System.out.println("over");
        }

        @Test
        public void renameFile() throws URISyntaxException, IOException, InterruptedException {
            FileSystem fs = FileSystem.get(
                    new URI("hdfs://hadoop102:9000"),
                    new Configuration(),
                    "lisi");
            fs.rename(new Path("/mayun.txt"),new Path("/mahuateng.txt"));
            fs.close();
            System.out.println("over");
        }

        @Test
        public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
            FileSystem fs = FileSystem.get(
                    new URI("hdfs://hadoop102:9000"),
                    new Configuration(),
                    "lisi");
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/test01"), true);
            while(files.hasNext()){
                LocatedFileStatus next = files.next();
                String name = next.getPath().getName();
                System.out.println("name="+name);

            }
            fs.close();
            System.out.println("over");
        }


        @Test
        public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
            FileSystem fs = FileSystem.get(
                    new URI("hdfs://hadoop102:9000"),
                    new Configuration(),
                    "lisi");
            FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
            for(FileStatus status:fileStatuses){
                if(status.isFile()){
                    System.out.println("文件名="+status.getPath().getName());
                }else{
                    System.out.println("文件夹名="+status.getPath().getName());
                }

            }
            fs.close();
            System.out.println("over");
        }

        @Test
        public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
            //获取文件系统
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000")
                    , new Configuration(), "lisi");
            //获取输入流
            FileInputStream fis = new FileInputStream(new File("d:/aaa.txt"));
            //获取输出流
            FSDataOutputStream fos = fs.create(new Path("/leijun.txt"));
            IOUtils.copyBytes(fis,fos,new Configuration());

            fs.close();
            System.out.println("over");
        }

        @Test
        public void downloadFile() throws URISyntaxException, IOException, InterruptedException {
            //获取文件系统
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000")
                    , new Configuration(), "lisi");
            //获取输入刘
            FSDataInputStream fis = fs.open(new Path("/liyanhong.txt"));
            FileOutputStream fos = new FileOutputStream(new File("/aaa2.txt"));
            IOUtils.copyBytes(fis,fos,new Configuration());
            fs.close();
        }
}
