package com.local.alam;

import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class RabbitConsumerDummy {
    // Defaults if environment variables are not set
    private static final String DEFAULT_LOG_FILE = "/var/log/message-consumed-from-RabbitMQ.log";
    private static final long DEFAULT_MAX_LOG_SIZE = 250 * 1024; // 250 KB

    public static void main(String[] argv) throws Exception {
        // RabbitMQ connection info from env
        String exchangeName = System.getenv().getOrDefault("RABBITMQ_EXCHANGE", "my_exchange");
        String queueName = System.getenv().getOrDefault("RABBITMQ_QUEUE", "my_queue");
        String host = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String user = System.getenv().getOrDefault("RABBITMQ_USER", "guest");
        String pass = System.getenv().getOrDefault("RABBITMQ_PASS", "guest");

        System.out.println("trying-to-connect-into:");
        System.out.println("RABBITMQ_HOST:"+host);
        System.out.println("RABBITMQ_USER:"+user);
        System.out.println("RABBITMQ_PASS:"+pass);
        System.out.println("RABBITMQ_QUEUE:"+queueName);

        // Log config from env
        String logFilePath = System.getenv().getOrDefault("LOG_FILE_PATH", DEFAULT_LOG_FILE);
        System.out.println("check-messages-log-on:"+logFilePath);
        long maxLogSize = Long.parseLong(
                System.getenv().getOrDefault("LOG_MAX_SIZE", String.valueOf(DEFAULT_MAX_LOG_SIZE))
        );

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(user);
        factory.setPassword(pass);

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        while (true) {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                // Declare exchange (pub-sub style)
                channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);

                // Declare queue and bind to exchange
                channel.queueDeclare(queueName, true, false, false, null);
                channel.queueBind(queueName, exchangeName, "");

                System.out.println(" [*] Subscribed to exchange: " + exchangeName + " via queue: " + queueName);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    String timestamp = LocalDateTime.now().format(dtf);

                    System.out.println(timestamp + " [x] Received: '" + message + "'");

                    rotateLogIfNeeded(logFilePath, maxLogSize);

                    try (FileWriter fw = new FileWriter(logFilePath, true);
                         PrintWriter pw = new PrintWriter(fw)) {
                        pw.println(timestamp + " [x] Received: '" + message + "'");
                    } catch (IOException e) {
                        System.err.println("Failed to write message to log file: " + e.getMessage());
                        System.out.println("Failed to write message to log file: " + e.getMessage());
                    }
                };

                // Keep consuming until connection breaks
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

                // Block thread so it keeps listening
                Thread.currentThread().join();

            } catch (Exception e) {
                System.err.println("Connection failed: " + e.getMessage());
                try {
                    Thread.sleep(5000); // wait before retry
                } catch (InterruptedException ignored) {}
            }
        }

    }

    private static void rotateLogIfNeeded(String logFilePath, long maxLogSize) {
        File logFile = new File(logFilePath);
        if (logFile.exists() && logFile.length() > maxLogSize) {
            String rotatedName = logFilePath + "." + System.currentTimeMillis();
            boolean success = logFile.renameTo(new File(rotatedName));
            if (success) {
                System.out.println(" [!] Log rotated: " + rotatedName);
            } else {
                System.err.println(" [!] Failed to rotate log file");
            }
        }
    }
}
