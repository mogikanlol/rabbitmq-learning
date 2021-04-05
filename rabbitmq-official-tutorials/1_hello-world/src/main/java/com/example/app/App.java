package com.example.app;

public class App {

    public static void main(String[] args) {

        final String queueName = "test-queue";

        Thread producerThread = new Thread(new Producer(queueName, "localhost"));
        Thread consumerThread = new Thread(new Consumer(queueName, "localhost"));

        producerThread.start();
        consumerThread.start();

    }

}
