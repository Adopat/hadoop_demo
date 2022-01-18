package com.imooc.mr;

import java.io.*;

/**
 * 使用Java 序列化生成文件，用于比较和Hadoop 序列化生成文件的区别，主要是存储空间大小,生成文件 StudentJava.txt 185字节
 * 单独执行时需要将pom.xml中的provide 属性注释，否则会提示找不到对应的包
 */
public class JavaSerialize {
    public static void main(String[] args) throws Exception {
        //创建对象
        StudentJava studentJava = new StudentJava(1L,"justin");
        // 序列化
        FileOutputStream fos = new FileOutputStream("E:\\StudentJava.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(studentJava);
        oos.close();
        fos.close();
        //反序列化,从文件中加载到内存
        FileInputStream fis = new FileInputStream("E:\\StudentJava.txt");
        ObjectInputStream ois = new ObjectInputStream(fis);
        StudentJava studentJava1 = (StudentJava) ois.readObject();
        System.out.println("反序列化后的对象是："+studentJava1);
        ois.close();
        fis.close();


    }
}

/**
 * 实现 Serializable 接口代表 StudentJava 类是可以序列化和反序列化的
 */
class StudentJava implements Serializable{
    // 设置版本号
    private static final long serialVersionUID = 1L;
    private Long id ;
    private String name;

    public StudentJava(Long id, String name) {
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
    public String toString() {
        return "StudentJava{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
