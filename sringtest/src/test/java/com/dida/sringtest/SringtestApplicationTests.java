package com.dida.sringtest;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@SpringBootTest
class SringtestApplicationTests {

    @Test
    void contextLoads() {
        System.out.println("开启线程");
    }

    /**
     * 线程池以及线程串行测试使用
     */
    //创建线程池，指定10个线程
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(20);
    @Test
    public void test() throws ExecutionException, InterruptedException {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>");
        System.out.println("zhang");
        CompletableFuture<Integer> integerCompletableFuture = CompletableFuture.supplyAsync(() -> {
            System.out.println("当前线程：" + Thread.currentThread().getId());
            int i = 100 / 2;
            return i;
        }, executor);

        System.out.println(integerCompletableFuture.get());
    }
}
