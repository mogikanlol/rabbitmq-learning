package com.example.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Producer {

    private final String producerName;
    private final String exchangeName;

    private Channel channel;

    public Producer(String producerName, String hostName, String exchangeName) {
        this.producerName = producerName;
        this.exchangeName = exchangeName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);

        try {
            Connection connection = factory.newConnection();
            this.channel = connection.createChannel();

            channel.exchangeDeclare(exchangeName, "fanout");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void publish(String message) {
        try {

            channel.basicPublish(
                    exchangeName,
                    "",
                    null,
                    message.getBytes(StandardCharsets.UTF_8)
            );

            System.out.println("["+ producerName + "] Sent '" + message + "'");

        } catch (IOException exception) {
            exception.printStackTrace();
        }

    }
}
