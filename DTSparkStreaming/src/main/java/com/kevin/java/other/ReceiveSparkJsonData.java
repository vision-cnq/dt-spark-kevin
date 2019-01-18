package com.kevin.java.other;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author kevin
 * @version 1.0
 * @description
 * @createDate 2019/1/17
 */
public class ReceiveSparkJsonData {

    /**
     * 加载json格式数据
     * @param url
     * @return
     */
    public static String loadJson (String url) {
        StringBuilder json = new StringBuilder();
        try {
            URL urlObject = new URL(url);
            URLConnection uc = urlObject.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
            String inputLine = null;
            while ( (inputLine = in.readLine()) != null) {
                json.append(inputLine);
            }
            in.close();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json.toString();
    }
    /**
     * 将字符串数据转换成json字符串格式的数据
     */
    public static String transformToJsonString(String string){
        return string.substring(1, string.length()-1).trim();
    }

    public static void main(String[] args) {
        String url = "http://localhost:4040/api/v1/applications";
        String result = loadJson(url);
        //[ {  "id" : "local-1507729315926",  "name" : "wc",
        //		"attempts" : [ {    "startTime" : "2017-10-11T13:41:47.856GMT",
        //			"endTime" : "1969-12-31T23:59:59.999GMT",    "sparkUser" : "",    "completed" : false  } ]} ]
        System.out.println(result);
        /**
         * 将result结果数据转换成json类型的字符串格式数据
         */
        String json = transformToJsonString(result);
        /**
         * 解析json格式的数据
         */
        JSONObject jsonObject;
        try{
            jsonObject = new JSONObject(json);
            //获取json中的字段值
            String name = jsonObject.getString("name");
            System.out.println("app name = "+name);
            //取得json数据中的json数据中的字段值
            JSONArray attempts = jsonObject.getJSONArray("attempts");
            System.out.println(attempts.toString());
            String newJson = transformToJsonString(attempts.toString());
            String startTime = new JSONObject(newJson).getString("startTime");
            System.out.println("startTime = "+startTime);
        }catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
