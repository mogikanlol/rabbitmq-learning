package com.example.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class Consumer implements Runnable {

    private final String queueName;
    private final String hostName;

    public Consumer(String queueName, String hostName) {
        this.queueName = queueName;
        this.hostName = hostName;
    }

    @Override
    public void run() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(hostName);

            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(queueName, false, false, false, null);

            System.out.println("[" + Thread.currentThread().getName() + "] Waiting for messages");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("[" + Thread.currentThread().getName() + "] Received '" + message + "'");
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
