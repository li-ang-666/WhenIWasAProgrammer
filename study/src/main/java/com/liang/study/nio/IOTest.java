package com.liang.study.nio;

import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;

public class IOTest {
    @Test
    public void test() throws Exception {
        FileChannelWriter fileChannelWriter = new FileChannelWriter("/Users/liang/Desktop/nio");
        MemoryMappedWriter memoryMappedWriter = new MemoryMappedWriter("/Users/liang/Desktop/mmap", false);
        PrintWriter printWriter = new PrintWriter(new FileWriter("/Users/liang/Desktop/io", true), true);
        Tester tester = new Tester();

        System.out.println("nio");
        tester.startTest(fileChannelWriter::write);
        tester.endTest(fileChannelWriter::close);

        System.out.println("mmap");
        tester.startTest(memoryMappedWriter::write);
        tester.endTest(() -> {
        });

        System.out.println("io");
        tester.startTest(printWriter::print);
        tester.endTest(printWriter::close);
    }
}

class Tester {
    private long time;

    public void startTest(ExecFunc func) {
        time = System.currentTimeMillis();
        for (long i = 1L; i <= 0.5 * 1000 * 1000 * 1000; i++) {
            if (i % (100 * 1000 * 1000) == 0) {
                System.out.println(i);
            }
            func.f("a");
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


