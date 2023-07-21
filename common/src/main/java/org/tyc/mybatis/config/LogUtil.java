package org.tyc.mybatis.config;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LogUtil {
    public static String toStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            e.printStackTrace(pw);
            return sw.toString();
        } catch (Exception e1) {
            return e1.getMessage();
        }
    }
}