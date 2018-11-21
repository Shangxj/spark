package com.sxj.util;

import java.io.*;

/**
 * @author sxj
 * @date 2018-10-29 18:46
 */
public class FileUtil {

    public static String ReadTxtFile(String pathName) {
        // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw
        /* 读入TXT文件 */
        // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径
//        String pathname = "D:\\twitter\\13_9_6\\dataset\\en\\input.txt";
        // 要读取以上路径的input。txt文件

        try {
            File filename = new File(pathName);
            // 建立一个输入流对象reader
            InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));

            // 建立一个对象，它把文件内容转成计算机能读懂的语言
            BufferedReader br = new BufferedReader(reader);
            String line = br.readLine();
            String tmpLine = "";
            while (line != null && tmpLine != "") {
                // 一次读入一行数据
                tmpLine = br.readLine();
            }
            return line + tmpLine;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void WriteTxtFile(String pathName, String txt) {
        BufferedWriter out = null;
        try {
            /* 写入Txt文件 */
            // 相对路径，如果没有则要建立一个新的output。txt文件
            File writename = new File(pathName);
            writename.createNewFile(); // 创建新文件
            out = new BufferedWriter(new FileWriter(writename));
            // \r\n即为换行
            out.write(txt);
            // 把缓存区内容压入文件

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.flush();
                // 最后记得关闭文件
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
//        FileUtil.WriteTxtFile("D://sxjj.txt", "你好");
        String txtFile = FileUtil.ReadTxtFile("D://sxjj.txt");
        System.out.println(txtFile);
    }
}
