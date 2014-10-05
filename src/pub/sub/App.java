package pub.sub;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;

public class App {

    private static final String EXCHANGE_NAME = "logs";
    private static final String EXCHANGE_TYPE = "fanout";
    private static final Logger LOGGER = new Logger();

    public static void main(String[] args) throws IOException {
        sendLogs();
        receiveLogs();
    }

    private static void receiveLogs() {
        ExecutorService threadPool = Executors.newFixedThreadPool(3);
        threadPool.submit(logger("FILE"));
        threadPool.submit(logger("DISK"));
        threadPool.submit(logger("WEB"));
    }

    private static Runnable logger(final String loggerType) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    Channel channel = createChannel();
                    channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
                    String queueName = channel.queueDeclare().getQueue();
                    channel.queueBind(queueName, EXCHANGE_NAME, "");
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(queueName, true, consumer);
                    while (true) {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                        String message = new String(delivery.getBody());
                        LOGGER.log(message, loggerType);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private static void sendLogs() throws IOException {
        final Channel channel = createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        Timer logSender = new Timer("LogSender");
        logSender.schedule(new TimerTask() {
            @Override
            public void run() {
                long second = System.currentTimeMillis() / 1000;
                String message = format("Current second: %d", second);
                try {
                    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                    LOGGER.log("Sent: " + message, "SENDER");
                } catch (IOException e) {
                    LOGGER.log("Failed to send: " + message, "SENDER");
                    e.printStackTrace();
                }
            }
        }, 0, 1000);
    }

    private static Channel createChannel() throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        return connection.createChannel();
    }
}

class Logger {
    private final Object lock = new Object();

    public void log(String msg, String type) {
        synchronized (lock) {
            System.out.printf("[%s]%s%n", type, msg);
        }
    }
}
