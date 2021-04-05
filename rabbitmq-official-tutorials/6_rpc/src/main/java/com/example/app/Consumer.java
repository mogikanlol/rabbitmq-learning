package com.example.app;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Consumer {

    private final ExecutorService executorService;

    private final String consumerName;
    private final String hostName;
    private final String queueName;

    private Connection connection;

    public Consumer(String consumerName, String hostName, String queueName) {
        executorService = Executors.newSingleThreadExecutor((r) -> new Thread(r, "Consumer Thread"));

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

        this.connection = factory.newConnection();
        Channel channel = connection.createChannel();


        channel.queueDeclare(queueName, false, false, false, null);
        channel.queuePurge(queueName);

        channel.basicQos(1);

        System.out.println("[" + consumerName + "] Waiting for messages");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {

            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[" + consumerName + "] Received '" + delivery.getEnvelope().getRoutingKey() + "':'"
                    + message + "'");

            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();
            String response = "";

            try {
                int n = Integer.parseInt(message);
                response += fib(n);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes(StandardCharsets.UTF_8));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                System.out.println("[" + consumerName + "] Sent answer in " + delivery.getProperties().getReplyTo() + " and correlationId = " + delivery.getProperties().getCorrelationId());
            }
        };

        boolean autoAck = false;

        channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> {
        });
    }

    public void stop() throws IOException {
        connection.close();
        executorService.shutdown();
    }

    private static int fib(int n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }

        return fib(n - 1) + fib(n - 2);
    }
}
