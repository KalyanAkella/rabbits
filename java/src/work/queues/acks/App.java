package work.queues.acks;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, InterruptedException {
        sendTasks("Hello World");
        executeTasks();
    }

    private static void sendTasks(final String message) throws IOException {
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
                    String body = message + "|" + System.currentTimeMillis();
                    channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
                    System.out.println(" [x] Sent '" + body + "'");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 500);
    }

    private static void executeTasks() throws IOException, InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        threadPool.submit(nonAckWorker());
        threadPool.submit(anAckWorker());
    }

    private static Runnable anAckWorker() throws IOException, InterruptedException {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    ConnectionFactory connectionFactory = new ConnectionFactory();
                    connectionFactory.setHost("localhost");
                    Connection connection = connectionFactory.newConnection();
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(QUEUE_NAME, false, consumer);

                    while (true) {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                        String message = new String(delivery.getBody());
                        System.out.println(" [x] Received ACK worker: '" + message + "'");
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private static Runnable nonAckWorker() throws IOException, InterruptedException {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    ConnectionFactory connectionFactory = new ConnectionFactory();
                    connectionFactory.setHost("localhost");
                    Connection connection = connectionFactory.newConnection();
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(QUEUE_NAME, false, consumer);

                    while (true) {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                        String message = new String(delivery.getBody());
                        System.out.println(" [x] Received Non ACK worker: '" + message + "'");
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
