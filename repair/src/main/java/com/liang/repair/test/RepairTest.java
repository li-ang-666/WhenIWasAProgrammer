package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Callable<String> task = () -> {
            // 模拟任务执行时间
            Thread.sleep(2000); // 任务执行超过1秒
            return "Task completed!";
        };

        Future<String> future = executor.submit(task);

        try {
            // 设置1秒的超时时间
            String result = future.get(1, TimeUnit.SECONDS);
            System.out.println(result);
        } catch (TimeoutException e) {
            System.err.println("任务执行超时！");
            future.cancel(true); // 超时后取消任务
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("任务执行失败: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}
