package com.sanjay.publisher.publishwithretry.service;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sanjay.publisher.publishwithretry.interfaces.MessageService;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.sanjay.publisher.publishwithretry.interfaces.Constants.QUEUE_NAME;

public class PublisherWithRetryService {

    private final MessageService messageService;
    private final Channel channel;
    private final Connection connection;

    public PublisherWithRetryService(
            MessageService messageService,
            ConfirmListener confirmListener
    ) throws IOException, TimeoutException {
        this.messageService = messageService;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Replace with the RabbitMQ server hostname

        // Create a connection
        connection = factory.newConnection();

        // Create a channel
        channel = connection.createChannel();

        // Declare a queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Enable publisher confirmation
        channel.confirmSelect();
        channel.addConfirmListener(confirmListener);
    }

    public void publish(String[] messages) throws IOException, TimeoutException {

        // Messages to be published
//        String[] messages = {"Hello", "RabbitMQ", "!"};

        for (String message : messages) {
            // Publish each message to the queue
            long deliveryTag = channel.getNextPublishSeqNo();
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            // Store the mapping between deliveryTag and message
            messageService.setMessage(deliveryTag, message);

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