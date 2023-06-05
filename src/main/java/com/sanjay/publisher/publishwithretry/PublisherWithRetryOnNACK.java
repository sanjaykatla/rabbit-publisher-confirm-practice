package com.sanjay.publisher.publishwithretry;


import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class PublisherWithRetryOnNACK {

    private final static String QUEUE_NAME = "my_queue";
    private final static int MAX_RETRY_ATTEMPTS = 3;
    private static Map<Long, String> deliveryTagToMessageMap = new HashMap<>();

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

                // Retry publishing the message
                retryPublish(channel, deliveryTag);

                String message = deliveryTagToMessageMap.get(deliveryTag);
                System.out.println("Message publishing failed. DeliveryTag: " + deliveryTag + ", Message: " + message);

                // Retry publishing the message
                retryPublish(channel, deliveryTag);
            }
        });

        // Messages to be published
        String[] messages = {"Hello", "RabbitMQ", "!"};

        for (String message : messages) {
            // Publish each message to the queue
            long deliveryTag = channel.getNextPublishSeqNo();
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            // Store the mapping between deliveryTag and message
            deliveryTagToMessageMap.put(deliveryTag, message);

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

    private static void retryPublish(Channel channel, long deliveryTag) throws IOException {
        int retryCount = 1;
        long retryDelay = 1000; // Initial retry delay in milliseconds

        while (retryCount <= MAX_RETRY_ATTEMPTS) {
            System.out.println("Retry attempt #" + retryCount);

            // Retrieve the message from deliveryTagToMessageMap
            String message = deliveryTagToMessageMap.get(deliveryTag);

            // Retry publishing the message
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

            try {
                // Wait for the publisher confirmation
                channel.waitForConfirmsOrDie();
                System.out.println("Message published successfully after retry. DeliveryTag: " + deliveryTag);
                return;
            } catch (InterruptedException e) {
                System.err.println("Message publishing interrupted during retry.");
            }

            // Exponential backoff delay between retries
            try {
                Thread.sleep(retryDelay);
            } catch (InterruptedException e) {
                System.err.println("Sleep interrupted during retry delay.");
            }

            retryCount++;
            retryDelay *= 2; // Double the retry delay for the next attempt
        }

        System.out.println("Reached maximum retry attempts. Message publishing failed. DeliveryTag: " + deliveryTag);
    }
}