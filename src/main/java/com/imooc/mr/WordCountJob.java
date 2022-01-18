package com.imooc.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

/**
 * 需求:读取hdfs 上的hello.txt文件,计算文件中每个单词出现的总次数
 *原始文件hello.txt内容如下：
 * hello you
 * hello me
 *
 * 最终需要的结果形式如下：
 * hello    2
 * me   1
 * you  1
 * hadoop 执行命令 hadoop jar hadoop_demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.imooc.mr.WordCountJob /test/hello.txt /out
 * mvn clean package -DskipTests （编译打包命令）
 */
public class WordCountJob {
    /**
     * Map阶段
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        // 使用Logger进行日志输出
        Logger logger = LoggerFactory.getLogger(MyMapper.class);
        /**
         * 接收<k1,v1> 输出 <k2,v2>
         * @param k1 代表行首的偏移量
         * @param v1 代表每一行数据
         * 对于输入分割中的每个键/值对调用一次。 大多数应用程序应该覆盖这个函数，但默认值是identity函数。
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException {
            // 输出 k1 ,v1 的值
            System.out.println("<k1,v1>=<"+k1.get()+","+v1.toString()+">");
            //使用log 输出
            logger.info("<k1,v1>=<"+k1.get()+","+v1.toString()+">");
            String[] words = v1.toString().split(" ");
            // 迭代切割出来的单词数据
            for(String word:words){
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                System.out.println("<k2:"+word+"...v2:1>");
                logger.info("<k2:"+word+"...v2:1>");
                context.write(k2,v2);
            }
        }
    }
    /**
     * Reduce 阶段
     */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        Logger logger = LoggerFactory.getLogger(MyReduce.class);
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
            //创建一个sum变量,保存v2s的值
            long sum = 0L;
            for(LongWritable v2:v2s){
                sum +=v2.get();
            }
            // 组装k3,v3
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            // 输出k3,v3的值
            System.out.println("<k3,v3>=<"+k3.toString()+","+v3.get()+">");
            // 使用Logger 进行日志输出
            logger.info("<k3,v3>=<"+k3.toString()+","+v3.get()+">");
            // 把结果写出去
            context.write(k3,v3);


        }
    }
    /**
     * 组装 Job = Map + Reduce
     */
    public static void main(String[] args) {
        try {
            if(args.length!=2){
                System.exit(100);
            }
            // 指定Job需要配置的参数
            Configuration conf = new Configuration();
            // 创建一个Job
            Job job = Job.getInstance(conf);
            //
            job.setJarByClass(WordCountJob.class);
            //指定输入路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            // 指定输出路径
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            // 指定map 相关的代码
            job.setMapperClass(MyMapper.class);
            // 指定 K2的类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v2的类型
            job.setMapOutputValueClass(LongWritable.class);
            // 指定reduce相关的代码
            job.setReducerClass(MyReduce.class);
            // 指定K3的类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v3的类型
            job.setOutputValueClass(LongWritable.class);
            // 提交Job
            job.waitForCompletion(true);

        }catch (Exception e){
            e.printStackTrace();
        }


    }



}
