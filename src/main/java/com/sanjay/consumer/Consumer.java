package com.sanjay.consumer;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class Consumer {

    private final static String QUEUE_NAME = "my_queue";

    public static void main(String[] args) throws Exception {
        // Create a connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Replace with the RabbitMQ container hostname

        // Create a connection
        Connection connection = factory.newConnection();

        // Create a channel
        Channel channel = connection.createChannel();

        // Declare a queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Create a consumer
        com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println("Received message: " + message);
            }
        };

        // Start consuming messages from the queue
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}


