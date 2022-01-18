package com.imooc.mr;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;

/**
 * 使用Hadoop 序列化生成文件，用于比较和Java 序列化生成文件的区别，主要是存储空间大小 生成文件 HadoopStudent.txt 22字节
 * 单独执行时需要将pom.xml中的provide 属性注释，否则会提示找不到对应的包
 */
public class HadoopSerialize {
    public static void main(String[] args){
        // 执行序列化方法
        //serialization();
        // 执行反序列化方法
        deserialization();
        //new ArrayList<>();
    }
    /**
     * 序列化方法 ,从内存保存到文件
     */
    public static  void serialization(){
        try (FileOutputStream fos = new FileOutputStream("E:\\HadoopStudent.txt");
             ObjectOutputStream oos = new ObjectOutputStream(fos)
                ){
            // 创建对象
            StudentWritable studentWritable = new StudentWritable(1L,"justin");
            studentWritable.write(oos);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    /**
     * 反序列化方法 ,从文件中加载到内存
     */
    public static void deserialization(){
        try (FileInputStream fis = new FileInputStream("E:\\HadoopStudent.txt");
             ObjectInputStream ois = new ObjectInputStream(fis)){
            StudentWritable studentWritable1 = new StudentWritable();
            studentWritable1.readFields(ois);
            System.out.println(studentWritable1);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
class StudentWritable implements Writable{
    private Long id;
    private String name;

    public StudentWritable() {
    }

    public StudentWritable(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();

    }

    @Override
    public String toString() {
        return "StudentWritable{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
