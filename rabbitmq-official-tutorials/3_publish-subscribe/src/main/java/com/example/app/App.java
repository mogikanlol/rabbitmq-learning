package com.example.app;

public class App {

    public static void main(String[] args) {
        final String hostName = "localhost";
        final String exchangeName = "logs";

        Consumer firstConsumer = new Consumer("C-1", hostName, exchangeName);
        Consumer secondConsumer = new Consumer("C-2", hostName, exchangeName);

        firstConsumer.start();
        secondConsumer.start();

        Producer producer = new Producer("P-1", hostName, exchangeName);

        for (int i = 0; i < 10; i++) {
            producer.publish(String.valueOf(i));
        }

    }

}
