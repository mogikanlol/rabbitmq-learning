package com.example.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumer {

    private final ExecutorService executorService;

    private final String consumerName;
    private final String hostName;
    private final String queueName;

    public Consumer(String consumerName, String hostName, String queueName) {
        executorService = Executors.newSingleThreadExecutor();

        this.consumerName = consumerName;
        this.hostName = hostName;
        this.queueName = queueName;
    }

    public void start() {

        Runnable task = () -> {
            try {
                consumeMessages();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        };

        executorService.submit(task);
    }

    private void consumeMessages() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        boolean durableQueue = true;

        channel.queueDeclare(queueName, durableQueue, false, false, null);

        // This tells RabbitMQ not to give more than one message to a worker at a time
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        System.out.println("[" + consumerName + "] Waiting for messages");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println("[" + consumerName + "] Received '" + message + "'");

            try {
                doWork(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                System.out.println("[" + consumerName + "] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        boolean autoAck = false;

        channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> { });
    }

    private void doWork(String message) throws InterruptedException {
        for (char c : message.toCharArray()) {
            if (c == '.') {
                TimeUnit.SECONDS.sleep(1L);
            }
        }
    }
}
