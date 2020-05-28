package com.kevin.java.utils;

import java.io.*;
import java.util.Date;
import java.util.UUID;

/**
 * @author kevin
 * @version 1.0
 * @description 此复制文件的程序是模拟在data目录下动态生成相同格式的txt文件，用于给sparkstreaming 中 textFileStream提供输入流。
 * @createDate 2019/1/14
 */
public class CopyFile_Data {

    public static void main(String[] args) throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(5000);
            //String uuid = UUID.randomUUID().toString();
            String file = "DTSparkStreaming\\src\\main\\resources\\";
            Date date = new Date();
            System.out.println(date.getTime());
            copyFile(new File(file+"words.txt"),new File(file+"data\\"+ date.getTime()+".txt"));
        }

    }

    public static void copyFile(File fromFile, File toFile) throws IOException {
        FileInputStream ins = new FileInputStream(fromFile);
        FileOutputStream out = new FileOutputStream(toFile);

        byte[] bytes = new byte[1024 * 1024];
        @SuppressWarnings("unused")
        int n = 0;
        while ((n = ins.read(bytes)) != -1) {
            out.write(bytes, 0, bytes.length);
        }

        ins.close();
        out.close();
    }
}
