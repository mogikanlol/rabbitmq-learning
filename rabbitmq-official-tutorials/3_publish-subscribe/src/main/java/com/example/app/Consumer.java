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
    private final String exchangeName;

    public Consumer(String consumerName, String hostName, String exchangeName) {
        executorService = Executors.newSingleThreadExecutor();

        this.consumerName = consumerName;
        this.hostName = hostName;
        this.exchangeName = exchangeName;
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

        channel.exchangeDeclare(exchangeName, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, exchangeName, "");

        System.out.println("[" + consumerName + "] Waiting for messages");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println("[" + consumerName + "] Received '" + message + "'");
        };

        boolean autoAck = true;

        channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> { });
    }
}
