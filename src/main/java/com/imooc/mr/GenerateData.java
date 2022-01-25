package com.imooc.mr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * 生成测试数据 验证 141M文件会产生两个 split,对应两个Map任务，140M文件产生一个split,一个Map任务
 */
public class GenerateData {
    public static void main(String[] args) throws Exception{
        // 生成141M文件
        //genderate_141();
        // 生成140M文件
        //genderate_140();
        //生成1000w条测试数据
        //genderate_1000w();
        // 生成smallfile文件夹
        genderate_smallFile();

    }
    private static void genderate_141() throws Exception{
        String fileName="E:\\testHadoop_141M.txt";
        System.out.println("start: 开始生成141M文件->"+fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num=0;
        while (num<11373017){
            bfw.write("hello world");//13个字节  (141*1024*1024)/13
            bfw.newLine();
            num++;
        }
        bfw.close();
        System.out.println("end: 141M文件已生成");

    }
    private static void genderate_140() throws Exception{
        String fileName="E:\\testHadoop_140M.txt";
        System.out.println("start: 开始生成140M文件->"+fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num=0;
        while (num<11292357){
            bfw.write("hello world");//13个字节  (140*1024*1024)/13
            bfw.newLine();
            num++;
        }
        bfw.close();
        System.out.println("end: 140M文件已生成");

    }
    /**
     * 生成测试数据，用于后期测试数据倾斜数据格式如下
     * 5 900W
     * 1-4 6-10 共100W
     */
    private static void genderate_1000w(){
        Random random =new Random();
        String fileName ="E:\\test_1000W.txt";
        System.out.println("start: 开始生成1000W条数据->"+fileName);
        try(FileWriter fileWriter = new FileWriter(fileName);
        BufferedWriter bfw =new BufferedWriter(fileWriter)){
        int num=0;
        while(num<10000000){
            int i = random.nextInt(10)+1;
            if(num<=999999){
                if(i!=5){
                    bfw.write(Integer.toString(i).concat(" bkadjfkfjkdfhksajdkfhdkfassdfdfdjbkkkkkkkkksaasd"));
                    bfw.newLine();
                    num++;
                }
            }else{
                bfw.write("5"+" bkadjfkfjkdfhksajdkfhdkfassdfdfdjbkkkkkkkkksaasd");
                bfw.newLine();
                num++;
            }
        }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 生成 50个小文件 做SequenceFile测试
     */
    private static void genderate_smallFile(){
       String fileDir="E:\\smallfile";
       File file = new File(fileDir);
       if(!file.exists()){
           file.mkdir();
       }
       int i=1;
       while(i<51){
           String fielAbsolutePath = fileDir+File.separator+"file"+i+".txt";
           try(FileWriter fw = new FileWriter(fielAbsolutePath);
               BufferedWriter bfw = new BufferedWriter(fw);
           ){
                bfw.write("hello world");
                i++;
           }catch (Exception e){
               e.printStackTrace();
           }
       }

    }
}
