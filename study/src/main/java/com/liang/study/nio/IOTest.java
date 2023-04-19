package com.liang.study.nio;

public class IOTest {
    @org.junit.Test
    public void test() throws Exception {
        FileChannelWriter fileChannelWriter = new FileChannelWriter("/Users/liang/Desktop/nio.txt");
        Tester tester = new Tester();
        tester.startTest(fileChannelWriter::write);
        tester.endTest(fileChannelWriter::close);
    }
}

class Tester {
    private long time;

    public void startTest(ExecFunc func) {
        time = System.currentTimeMillis();
        for (int i = 1; i <= 100000000 / 2; i++) {
            if (i % 10000000 == 0) {
                System.out.println(i);
            }
            func.f("我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上我的家在东北松花江上\n");
        }
    }

    public void endTest(CloseFunc func) {
        func.f();
        System.out.println("耗时" + (System.currentTimeMillis() - time) / 1000 + "秒");
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


