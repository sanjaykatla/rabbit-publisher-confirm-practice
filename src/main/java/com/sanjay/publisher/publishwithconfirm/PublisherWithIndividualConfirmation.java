package com.sanjay.publisher.publishwithconfirm;


import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class PublisherWithIndividualConfirmation {

    private final static String QUEUE_NAME = "my_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        // Create a connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Replace with the RabbitMQ server hostname

        // Create a connection
        Connection connection = factory.newConnection();

        // Create a channel
        Channel channel = connection.createChannel();

        // Declare a queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Enable publisher confirmation
        channel.confirmSelect();

        // Add a listener for publisher confirmations
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Message published successfully. DeliveryTag: " + deliveryTag);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Message publishing failed. DeliveryTag: " + deliveryTag);
            }
        });

        // Messages to be published
        String[] messages = {"Hello", "RabbitMQ", "!"};

        for (String message : messages) {
            // Publish each message to the queue
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

            try {
                // Wait for the publisher confirmation for each message
                channel.waitForConfirmsOrDie();
            } catch (InterruptedException e) {
                System.err.println("Message publishing interrupted.");
            }

            System.out.println("Message sent: " + message);
        }

        // Close the channel and connection
        channel.close();
        connection.close();
    }
}

