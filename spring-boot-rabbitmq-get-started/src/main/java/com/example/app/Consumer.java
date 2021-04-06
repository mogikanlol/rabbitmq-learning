package com.example.app;

import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class Consumer {

    private CountDownLatch latch = new CountDownLatch(1);

    public void consumeMessage(String message) {
        System.out.println("Consumed <" + message + ">");
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
