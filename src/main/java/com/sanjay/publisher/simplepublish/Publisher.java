package com.sanjay.publisher.simplepublish;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

public class Publisher {

    private final static String QUEUE_NAME = "my_queue";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();


        // Create a channel
        Channel channel = connection.createChannel();

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

        // Declare a queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Message to be published
        String message = "Hello, RabbitMQ!";
        String message2 = "Hello, RabbitMQ2!";

        // Publish the message to the queue
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        channel.basicPublish("", QUEUE_NAME, null, message2.getBytes());
        System.out.println("Message sent: " + message);
        System.out.println("Message2 sent: " + message2);

        // Close the channel and connection
        channel.close();
        connection.close();
    }
}


