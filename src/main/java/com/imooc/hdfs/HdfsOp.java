package com.imooc.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Java 代码操作HDFS，单独执行时需要将pom.xml中的provide 属性注释，否则会提示找不到对应的包
 * 文件操作:上传文件，下载文件，删除文件
 */
public class HdfsOp {
    public static void main(String[] args) throws Exception{
        // 创建一个配置对象
        Configuration conf = new Configuration();
        // 指定HDFS的地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        // 获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        //get(fileSystem);
        // 上传文件
       //put(fileSystem,"user.txt","D:\\user.txt");
       // 下载文件
        //get(fileSystem,"/test/user.txt","D:\\user_bak.txt");
        // 删除文件
        delete(fileSystem,"/test/user.txt");


    }

    /**
     *文件上传
     * @param fileSystem
     * @param outputPath 文件输出路径
     * @param inputPath 读取文件路劲
     * @throws IOException
     */
    public static void put(FileSystem fileSystem,String outputPath,String inputPath) throws IOException{
        // 获取hdfs文件系统输出流
        //FSDataOutputStream fos = fileSystem.create(new Path("/user.txt"));
        FSDataOutputStream fos = fileSystem.create(new Path("/test/",outputPath));
        // 获取本地文件输入流
        //FileInputStream fis = new FileInputStream("D:\\user.txt");
        FileInputStream fis = new FileInputStream(inputPath);
        IOUtils.copyBytes(fis,fos,1024,true);
    }

    /**
     * 文件下载
     * @param fileSystem hdfs 文件系统操作对象
     * @param inputPath 文件读取路劲
     * @param outputPath 文件下载路径
     * @throws IOException
     */
    public static void get(FileSystem fileSystem,String inputPath,String outputPath) throws IOException{
        // 获取HDFS文件系统输入流
        FSDataInputStream fis = fileSystem.open(new Path(inputPath));
        // 获取本地文件的输出流
        FileOutputStream fos = new FileOutputStream(outputPath);
        //下载文件
        IOUtils.copyBytes(fis,fos,1024,true);
    }

    /**
     * 文件删除
     * @param fileSystem
     * @param deletePath 删除文件路径
     * @throws IOException
     */
    private static void delete(FileSystem fileSystem,String deletePath) throws IOException{
        boolean flag = fileSystem.delete(new Path(deletePath),true);
        if(flag){
            System.out.println("删除成功!!!");
        }else{
            System.out.println("删除失败!!!");
        }
    }
}
