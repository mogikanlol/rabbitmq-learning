package com.example.app;

import java.util.List;

public class App {

    public static void main(String[] args) {
        final String hostName = "localhost";
        final String exchangeName = "direct_logs";

        Consumer firstConsumer = new Consumer(
                "C-1",
                hostName,
                exchangeName,
                List.of("info", "warning", "error")
        );
        Consumer secondConsumer = new Consumer(
                "C-2",
                hostName,
                exchangeName,
                List.of("error")
        );

        firstConsumer.start();
        secondConsumer.start();

        Producer producer = new Producer("P-1", hostName, exchangeName);

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                producer.publish(String.valueOf(i), "error");
            } else if (i % 3 == 0) {
                producer.publish(String.valueOf(i), "warning");
            } else {
                producer.publish(String.valueOf(i), "info");
            }
        }

    }

}
