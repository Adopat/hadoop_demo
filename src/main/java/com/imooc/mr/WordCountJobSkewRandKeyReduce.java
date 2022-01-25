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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;


/**
 * 数据倾斜 增加Reduce任务个数，对比不增加Reduce 消耗时间(对于数据倾斜太大的数据,增加Reduce个数提升效果不大)
 * 数据倾斜，将数据打散重新分区(在这里我们指导5是最多的，在实际开发中可以进行抽样观察)
 * 数据格式 5 bkadjfkfjkdfhksajdkfhdkfassdfdfdjbkkkkkkkkksaasd
 * 其中 5-900W 1-4 6-10 共100w
 * hadoop 执行命令 hadoop jar hadoop_demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.imooc.mr.WordCountJobSkewRandKeyReduce /out1000_randomKey/part-r-* /out1000_randomKey_Reduce 1
 * 10个ReduceTask 执行 hadoop jar hadoop_demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.imooc.mr.WordCountJobSkewRandKeyReduce /out1000_randomKey/part-r-* /out1000_randomKey_Reduce 10
 * mvn clean package -DskipTests （编译打包命令）
 */
public class WordCountJobSkewRandKeyReduce {
    /**
     * Map阶段
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        // 使用Logger进行日志输出
        //Logger logger = LoggerFactory.getLogger(MyMapper.class);
        Random random = new Random();
        /**输入文件part-r-* 上一个打散结果的输出文件 在进行一次汇总
         * 接收<k1,v1> 输出 <k2,v2>
         * @param k1 代表行首的偏移量
         * @param v1 代表每一行数据
         * 对于输入分割中的每个键/值对调用一次。 大多数应用程序应该覆盖这个函数。
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException {
            // 输出 k1 ,v1 的值
            //System.out.println("<k1,v1>=<"+k1.get()+","+v1.toString()+">");
            //使用log 输出
           // logger.info("<k1,v1>=<"+k1.get()+","+v1.toString()+">");
            // 以制表符作为切割
            String[] words = v1.toString().split("\t");
            // 切割出来的数据我们只需要words[0]就可以了，主要是统计数字出现的次数
            // 截取需要的数字
            String key = words[0].split("_")[0];
            Text k2 = new Text(key);
            //System.out.println("k2 ="+k2.toString()+"v2="+words[1]);
            LongWritable v2 = new LongWritable((Long.parseLong(words[1])));
            // 把<k2.v2>写出
            context.write(k2,v2);

        }
    }
    /**
     * Reduce 阶段
     */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        //Logger logger = LoggerFactory.getLogger(MyReduce.class);

        /**
         * 针对<k2,{v2...}>的数据进行累加求和，并且最终把数据转化为k3,v3写出去
         * @param k2
         * @param v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
            //创建一个sum变量,保存v2s的值
            long sum = 0L;
            for(LongWritable v2:v2s){
                sum +=v2.get();
                //模拟Reduce的复杂计算消耗的时间
                if(sum%200 == 0){
                    Thread.sleep(1);
                }
            }
            // 组装k3,v3
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            // 输出k3,v3的值
            //System.out.println("<k3,v3>=<"+k3.toString()+","+v3.get()+">");
            // 使用Logger 进行日志输出
            //logger.info("<k3,v3>=<"+k3.toString()+","+v3.get()+">");
            // 把结果写出去
            context.write(k3,v3);


        }
    }
    /**
     * 组装 Job = Map + Reduce
     */
    public static void main(String[] args) {
        try {
            if(args.length!=3){
                System.exit(100);
            }
            // 指定Job需要配置的参数
            Configuration conf = new Configuration();
            // 创建一个Job
            Job job = Job.getInstance(conf);
            //注意了：这一行必须设置，否则在集群中执行的时候是找不到WordCountJobSkewRandKeyReduce这个类的
            job.setJarByClass(WordCountJobSkewRandKeyReduce.class);
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
            // 设置Reducer任务个数,第三个参数为输入的Reduce个数
            job.setNumReduceTasks(Integer.parseInt(args[2]));
            //获取分区信息
            //job.getPartitionerClass();
            // 提交Job
            job.waitForCompletion(true);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
