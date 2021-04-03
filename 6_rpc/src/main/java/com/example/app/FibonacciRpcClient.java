package com.example.app;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FibonacciRpcClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;

    private final String producerName;
    private final String hostName;
    private final String queueName;

    public FibonacciRpcClient(String producerName, String hostName, String queueName) {
        this.producerName = producerName;
        this.hostName = hostName;
        this.queueName = queueName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);
        try {
            this.connection = factory.newConnection();
            this.channel = connection.createChannel();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public String call(String message) throws Exception {
        final String callbackQueueName = channel.queueDeclare().getQueue();
        final String correlationId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(correlationId)
                .replyTo(callbackQueueName)
                .build();

        channel.basicPublish(
                "",
                queueName,
                props,
                message.getBytes(StandardCharsets.UTF_8)
        );

        System.out.println("[" + producerName + "] Sent '" + queueName + "':'" + message + "'");
        System.out.println("[" + producerName + "] CallBackQueueName = " + callbackQueueName + " correlationId = " + correlationId);

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(callbackQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                response.offer(new String(delivery.getBody(), StandardCharsets.UTF_8));
            }
        }, consumerTag -> {
        });

        String result = response.take();
        channel.basicCancel(ctag);

        return result;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
