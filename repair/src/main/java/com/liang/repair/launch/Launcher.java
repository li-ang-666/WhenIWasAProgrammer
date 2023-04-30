package com.liang.repair.launch;

import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Launcher {
    public static void main(String[] args) {
        try {
            Object instance = Class.forName("com.liang.repair.impl." + args[0]).newInstance();
            ((Runner) instance).run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
