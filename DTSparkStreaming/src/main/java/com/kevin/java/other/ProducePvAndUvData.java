package com.kevin.java.other;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Random;

/**
 * @author kevin
 * @version 1.0
 * @description     模拟生成网站访问数据
 * @createDate 2019/1/17
 */
public class ProducePvAndUvData {

    //ip
    public static Integer IP = 223;
    //地址
    public static String[] ADDRESS = {
            "北京","天津","上海","重庆","河北","山西","辽宁","吉林","江苏","浙江","黑龙江",
            "安徽","福建","江西","山东","河南","湖北","湖南","广东","海南","四川","贵州",
            "云南","山西","甘肃","青海","台湾","内蒙","广西","西藏","宁夏","新疆","香港","澳门"};
    //日期
    public static String DATE = "2019-01-17";
    //timestamp
    public static Long TIMESTAMP = 0L;
    //userid
    public static Long USERID = 0L;
    //网站
    public static String[] WEBSITE = {"www.baidu.com","www.taobao.com","www.dangdang.com",
            "www.jd.com","www.suning.com","www.mi.com","www.gome.com.cn"};
    //行为
    public static String[] ACTION = {"Regist","Comment","View","Login","Buy","Click","Logout"};

    /**
     * 创建文件
     */
    public static Boolean CreateFile(String pathFileName){
        try {
            File file = new File(pathFileName);
            if(file.exists()){
                file.delete();
            }
            boolean createNewFile = file.createNewFile();
            System.out.println("create file "+pathFileName+" success!");
            return createNewFile;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 向文件中写入数据
     */
    public static void WriteDataToFile(String pathFileName,String newContent){
        FileOutputStream fos = null ;
        OutputStreamWriter osw = null;
        PrintWriter pw = null ;
        try {
            //产生一行模拟数据
            String content = newContent;
            File file = new File(pathFileName);
            fos=new FileOutputStream(file,true);
            osw=new OutputStreamWriter(fos, "UTF-8");
            pw =new PrintWriter(osw);
            pw.write(content+"\n");
            //注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
            pw.close();
            osw.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String pathFileName = "./DTSparkStreaming/pvuvdata";
        //创建文件
        Boolean createFile = CreateFile(pathFileName);
        if(createFile){
            int i = 0;
            //产生5万+数据
            while(i<50000){
                //模拟一个ip
                Random random = new Random();
                String ip = random.nextInt(IP)+"."+random.nextInt(IP)+"."+random.nextInt(IP)+"."+random.nextInt(IP);
                //模拟地址
                String address = ADDRESS[random.nextInt(34)];
                //模拟日期
                String date = DATE;
                //模拟userid
                Long userid =  Math.abs(random.nextLong());
                /**
                 * 这里的while模拟是同一个用户不同时间点对不同网站的操作
                 */
                int j = 0 ;
                Long timestamp = 0L;
                String webSite = "未知网站";
                String action = "未知行为";
                int flag = random.nextInt(5)|1;
                while(j<flag){
//					Threads.sleep(5);
                    //模拟timestamp
                    timestamp = new Date().getTime();
                    //模拟网站
                    webSite = WEBSITE[random.nextInt(7)];
                    //模拟行为
                    action = ACTION[random.nextInt(6)];
                    j++;
                    /**
                     * 拼装
                     */
                    String content = ip+"\t"+address+"\t"+date+"\t"+timestamp+"\t"+userid+"\t"+webSite+"\t"+action;
                    System.out.println(content);
                    //向文件中写入数据
                    WriteDataToFile(pathFileName,content);
                }
                i++;
            }

        }
    }
}
