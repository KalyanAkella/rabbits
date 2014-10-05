package work.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class App {
    private static final String QUEUE_NAME = "hello";
    private static final int NUM_WORKERS = 10;

    public static void main(String[] args) throws IOException {
        sendMessages("Hello World");
        receiveMessages();
    }

    private static void sendMessages(final String message) throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        Timer messageSender = new Timer("MessageSender");
        messageSender.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                    System.out.println(" [x] Sent '" + message + "'");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 1);
    }

    private static void receiveMessages() throws IOException {
        for (int i = 1; i <= NUM_WORKERS; i++) {
            new Thread(new Worker(i)).start();
        }
    }

    private static class Worker implements Runnable {
        private final int id;

        private Worker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                ConnectionFactory connectionFactory = new ConnectionFactory();
                connectionFactory.setHost("localhost");
                Connection connection = connectionFactory.newConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                final QueueingConsumer consumer = new QueueingConsumer(channel);
                channel.basicConsume(QUEUE_NAME, true, consumer);
                while (true) {
                    try {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                        String message = new String(delivery.getBody());
                        System.out.println(" [x] Received in " + id + "'" + message + "'");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}
