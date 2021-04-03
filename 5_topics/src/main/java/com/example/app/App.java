package com.example.app;

import java.util.List;

public class App {

    public static void main(String[] args) {
        final String hostName = "localhost";
        final String exchangeName = "topic_logs";

        Consumer firstConsumer = new Consumer(
                "C-1",
                hostName,
                exchangeName,
                List.of("cron.*")
        );
        Consumer secondConsumer = new Consumer(
                "C-2",
                hostName,
                exchangeName,
                List.of("kern.*", "*.critical")
        );
        Consumer thirdConsumer = new Consumer(
                "C-3",
                hostName,
                exchangeName,
                List.of("#")
        );

        firstConsumer.start();
        secondConsumer.start();
        thirdConsumer.start();

        Producer producer = new Producer("P-1", hostName, exchangeName);

        for (int i = 0; i < 5; i++) {
            producer.publish("A critical kernel error - " + i, "kern.critical");
        }


        for (int i = 0; i < 3; i++) {
            producer.publish("A debug message from cron - " + i, "cron.debug");
        }


    }

}
