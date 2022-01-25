package com.imooc.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;

/**
 * 小文件解决方案MapFile
 */
public class SmallFileMap {
    public static void main(String[] args) throws Exception{
        //生成MapFile文件
        write("E:\\smallfile","/mapFile");
        // 读取MapFile 文件
        read("/mapFile");

    }
    /**
     * 生成MapFile 文件
     * @param inputDir 输入目录-windows目录
     * @param outputDir 输出目录-hdfs目录
     */
    private static void write(String inputDir,String outputDir) throws Exception{
        // 创建配置对象
        Configuration conf = new Configuration();
        // 指定HDFS的地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        // 获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        // 删除HDFS 上的输出文件 保证函数可以重复执行
        fileSystem.delete(new Path(outputDir),true);
        /**
         * 构造opts 数组
         * 第一个参数 key 文件名
         * 第二个参数 value 文件内容
         */
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };
        // 创建一个writer实例
        MapFile.Writer writer = new MapFile.Writer(conf, new Path(outputDir), opts);
        // 指定需要压缩的文件的目录
        File inputDirPath = new File(inputDir);
        if(inputDirPath.isDirectory()){
            File[] files = inputDirPath.listFiles();
            // 迭代文件
            for(File file:files){
                // 获取文件的全部内容
                String content = FileUtils.readFileToString(file,"UTF-8");
                // 获取文件名
                String fileName = file.getName();
                // 获取 key value key 为文件名 value 为文件内容
                Text key = new Text(fileName);
                Text value = new Text(content);
                // 向SequenceFile中写入数据
                writer.append(key,value);
            }
        }
        writer.close();
    }

    /**
     * 读取MapFile文件路径
     * @param inputDir  MapFile文件路径
     */
    private static void read(String inputDir) throws Exception{
        // 创建一个配置对象
        Configuration conf = new Configuration();
        // 指定HDFS的地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        // 创建阅读器
        MapFile.Reader reader = new MapFile.Reader(new Path(inputDir),conf);
        Text key = new Text();
        Text value = new Text();
        // 循环读取数据
        while(reader.next(key,value)){
            //输出文件名称
            System.out.print("文件名："+key.toString()+",");
            //输出文件内容
            System.out.println("文件内容："+value.toString()+"");
        }
        reader.close();

    }
}
