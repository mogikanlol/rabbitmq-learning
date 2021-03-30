package com.example.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Producer implements Runnable {

    private final String queueName;
    private final String hostName;

    public Producer(String queueName, String hostName) {
        this.queueName = queueName;
        this.hostName = hostName;
    }

    @Override
    public void run() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()
        ) {
            channel.queueDeclare(queueName, false, false, false, null);
            String message = "Hello, World!";
            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("[" + Thread.currentThread().getName() + "] Message was sent");

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }
}
