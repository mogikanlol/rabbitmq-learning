package com.example.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

public class App {

    private static final ConnectionFactory CONNECTION_FACTORY;
    private static final int MESSAGES_COUNT = 50_000;

    static {
        CONNECTION_FACTORY = new ConnectionFactory();
        CONNECTION_FACTORY.setHost("localhost");
        CONNECTION_FACTORY.setUsername("guest");
        CONNECTION_FACTORY.setPassword("guest");
    }

    public static void main(String[] args) throws Exception {
        publishMessagesIndividually();
        publishMessagesInBatch();
        handlePublishConfirmsAsynchronously();
    }

    private static void publishMessagesIndividually() throws Exception {
        try (Connection connection = CONNECTION_FACTORY.newConnection()) {
            Channel channel = connection.createChannel();

            String queueName = UUID.randomUUID().toString();
            channel.queueDeclare(queueName, false, false, true, null);

            channel.confirmSelect();

            System.out.println("--- Started to publish messages with individual confirm");
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGES_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", queueName, null, body.getBytes());
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGES_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    private static void publishMessagesInBatch() throws Exception {
        try (Connection connection = CONNECTION_FACTORY.newConnection()) {
            Channel channel = connection.createChannel();

            String queueName = UUID.randomUUID().toString();
            channel.queueDeclare(queueName, false, false, true, null);

            channel.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            System.out.println("--- Started to publish messages with batch confirm");
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGES_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", queueName, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    channel.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                System.out.println("strange if statement");
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGES_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    private static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = CONNECTION_FACTORY.newConnection()) {
            Channel channel = connection.createChannel();

            String queueName = UUID.randomUUID().toString();
            channel.queueDeclare(queueName, false, false, true, null);

            channel.confirmSelect();

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            ConfirmCallback cleanOutstandingConfirms = (deliveryTag, multiple) -> {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(deliveryTag, true);
                    confirmed.clear();
                } else {
                    outstandingConfirms.remove(deliveryTag);
                }
            };

            channel.addConfirmListener(cleanOutstandingConfirms, (deliveryTag, multiple) -> {
                String body = outstandingConfirms.get(deliveryTag);
                System.err.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple %b%n",
                        body, deliveryTag, multiple
                );
                cleanOutstandingConfirms.handle(deliveryTag, multiple);
            });

            System.out.println("--- Started to publish messages with async confirm");
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGES_COUNT; i++) {
                String body = String.valueOf(i);

                outstandingConfirms.put(channel.getNextPublishSeqNo(), body);

                channel.basicPublish("", queueName, null, body.getBytes());
            }

            if (!waitUntil(Duration.ofSeconds(60), outstandingConfirms::isEmpty)) {
                throw new IllegalStateException("All messages count not be confirmed in 60 seconds");
            }

            long end = System.nanoTime();
            System.out.format(
                    "Published %,d messages and handled confirms asynchronously in %,d ms%n",
                    MESSAGES_COUNT, Duration.ofNanos(end - start).toMillis()
            );
        }
    }

    private static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() || waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited += 100;
        }
        return condition.getAsBoolean();
    }
}
