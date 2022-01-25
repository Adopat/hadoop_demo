package com.imooc.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * job 任务只有Map阶段没有Reduce 阶段
 */
public class WordCoutJobNoReduce {
    /**
     * Map阶段
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
        // 使用Logger进行日志输出
        Logger logger = LoggerFactory.getLogger(WordCountJob.MyMapper.class);
        /**
         * 接收<k1,v1> 输出 <k2,v2>
         * @param k1 代表行首的偏移量
         * @param v1 代表每一行数据
         * 对于输入分割中的每个键/值对调用一次。
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
     * 组装 Job = Map
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
            job.setJarByClass(WordCoutJobNoReduce.class);
            //指定输入路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            // 指定输出路径
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            // 指定map 相关的代码
            job.setMapperClass(WordCoutJobNoReduce.MyMapper.class);
            // 指定 K2的类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v2的类型
            job.setMapOutputValueClass(LongWritable.class);
            // 设置ReduceTask
            job.setNumReduceTasks(0);
            // 提交Job
            job.waitForCompletion(true);

        }catch (Exception e){
            e.printStackTrace();
        }


    }



}
