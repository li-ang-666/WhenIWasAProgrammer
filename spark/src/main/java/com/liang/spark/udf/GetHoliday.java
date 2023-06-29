package com.liang.spark.udf;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class GetHoliday /*extends UDF*/ {
    public String evaluate(String dt) {
        if (dt == null || dt.trim().isEmpty())
            return null;
        try {
            String v = this.request(dt.replace("/", "").replace("-", "").trim());
            return v;
        } catch (Exception cc) {
            System.out.println(cc.getMessage());
            return null;
        }
    }

    public String request(String dt) {
        String httpUrl = "http://tool.bjzrch.com/jiari/";
        BufferedReader reader = null;
        String result = null;
        StringBuffer sbf = new StringBuffer();
        httpUrl = httpUrl + "?d=" + dt;
        try {
            URL url = new URL(httpUrl);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            InputStream is = connection.getInputStream();
            reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String strRead = null;
            while ((strRead = reader.readLine()) != null) {
                sbf.append(strRead);
                sbf.append("\r\n");
            }
            reader.close();
            result = sbf.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(new GetHoliday().request("2023-05-01"));
    }
}