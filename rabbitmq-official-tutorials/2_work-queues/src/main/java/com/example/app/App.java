package com.example.app;

public class App {

    public static void main(String[] args) {
        final String hostName = "localhost";
        final String queueName = "test-queue";

        Consumer firstConsumer = new Consumer("C-1", hostName, queueName);
        Consumer secondConsumer = new Consumer("C-2", hostName, queueName);

        firstConsumer.start();
        secondConsumer.start();

        Producer producer = new Producer("P-1", hostName, queueName);

        for (int i = 0; i < 10; i++) {
            producer.publish(i + "...");
        }

    }

}
