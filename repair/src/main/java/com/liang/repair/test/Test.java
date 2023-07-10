package com.liang.repair.test;

import lombok.AllArgsConstructor;
import lombok.Data;

public class Test {
    public static void main(String[] args) throws Exception {
        Student stu = new Student(1, "tom");
        ff(stu.id);
        System.out.println(stu);
    }

    public static void f(Student arg) {
        //arg.name=null;
        //arg=null;
    }

    public static void ff(int i) {
        i = 5;
    }

    @Data
    @AllArgsConstructor
    private static class Student {
        public int id;
        public String name;
    }
}
