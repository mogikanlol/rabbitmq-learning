package com.example.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer {

    private final ExecutorService executorService;

    private final String consumerName;
    private final String hostName;
    private final String exchangeName;
    private final List<String> routingKeys;

    public Consumer(String consumerName, String hostName, String exchangeName, List<String> routingKeys) {
        executorService = Executors.newSingleThreadExecutor();

        this.consumerName = consumerName;
        this.hostName = hostName;
        this.exchangeName = exchangeName;
        this.routingKeys = routingKeys;
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

        channel.exchangeDeclare(exchangeName, "direct");
        String queueName = channel.queueDeclare().getQueue();

        for (String routingKey : routingKeys) {
            channel.queueBind(queueName, exchangeName, routingKey);
        }

        System.out.println("[" + consumerName + "] Waiting for messages");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println("[" + consumerName + "] Received '" + delivery.getEnvelope().getRoutingKey() + "':'"
                    + message + "'");
        };

        boolean autoAck = true;

        channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> { });
    }
}
