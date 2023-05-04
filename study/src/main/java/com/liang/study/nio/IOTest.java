package com.liang.study.nio;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

public class IOTest {

    @Test
    public void test() throws Exception {
        //删除文件
        new File("/Users/liang/Desktop/nio").delete();
        new File("/Users/liang/Desktop/mmap").delete();
        new File("/Users/liang/Desktop/io").delete();
        //创建流
        FileChannelWriter fileChannelWriter = new FileChannelWriter("/Users/liang/Desktop/nio");
        MemoryMappedWriter memoryMappedWriter = new MemoryMappedWriter("/Users/liang/Desktop/mmap");
        PrintWriter printWriter = new PrintWriter(new FileWriter("/Users/liang/Desktop/io", true), true);
        //测试器
        Tester tester = new Tester();
        //do test
        System.out.println("nio");
        tester.startTest(fileChannelWriter::write);
        tester.endTest(fileChannelWriter::close);

        System.out.println("mmap");
        tester.startTest(memoryMappedWriter::write);
        tester.endTest(memoryMappedWriter::close);

        System.out.println("io");
        tester.startTest(printWriter::print);
        tester.endTest(printWriter::close);
    }
}

class Tester {
    private long time;

    public void startTest(ExecFunc func) {
        String content = "time = 2022-02-02 22:22:22, partition = 22, offset = 2222222, orgId = xiaomi, table = applications, dml = DELETE, id = 2222222, created_at = 2022-02-02 22:22:22, updated_at = 2022-02-02 22:22:22\n";
        time = System.currentTimeMillis();
        int times = 500000;//5w 50w 500w 1000w
        for (int i = 1; i <= times; i++) {
            func.f(content);
        }
    }

    public void endTest(CloseFunc func) {
        func.f();
        System.out.println("耗时" + (System.currentTimeMillis() - time) / 1000.0 + "秒");
    }
}

@FunctionalInterface
interface ExecFunc {
    void f(String s);
}

@FunctionalInterface
interface CloseFunc {
    void f();
}


