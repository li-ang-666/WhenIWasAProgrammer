package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Stream;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) {
        System.out.println(Stream.of(
                75356550,
                75220849,
                75276705,
                75308659,
                75315607,
                75281967,
                75289875,
                75277226,
                75288175,
                75337035
        ).mapToLong(e -> e).sum());
    }
}
