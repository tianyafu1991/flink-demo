package com.youfan.flink.dsinfoservice;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class DsClienttest {

    public static void main(String[] args) {
        String message = "test";
        String address = "http://localhost:6097/dsInfo/webInfoSJService";
        try {
            URL url = new URL(address);
            HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();
            urlConnection.setRequestMethod("POST");
            urlConnection.setDoOutput(true);
            urlConnection.setDoInput(true);
            urlConnection.setAllowUserInteraction(true);
            urlConnection.setUseCaches(false);
            urlConnection.setReadTimeout(6*1000);
            urlConnection.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36");
            urlConnection.setRequestProperty("Content-Type","application/json");
            urlConnection.connect();
            OutputStream outputStream = urlConnection.getOutputStream();
            BufferedOutputStream out = new BufferedOutputStream(outputStream);
            out.write(message.getBytes());
            out.flush();

            String temp = "";
            InputStream in = urlConnection.getInputStream();
            byte[] tempbytes = new byte[1024];
            while (in.read(tempbytes,0,1024)!=-1){
                temp += new String(tempbytes);
            }
            System.out.println(urlConnection.getResponseCode());
            System.out.println(temp);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
