package routing;

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
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String EXCHANGE_TYPE = "direct";
    private static final Logger LOGGER = new Logger();
    private static final String[] LEVELS = new String[] { "debug", "info", "warning", "error" };

    public static void main(String[] args) throws IOException {
        sendLogs();
        receiveLogs();
    }

    private static void receiveLogs() {
        ExecutorService threadPool = Executors.newFixedThreadPool(LEVELS.length);
        for (String level : LEVELS) {
            threadPool.submit(logger(level));
        }
    }

    private static Runnable logger(final String logLevel) {
        return () -> {
            try {
                Channel channel = createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, EXCHANGE_NAME, logLevel);
                QueueingConsumer consumer = new QueueingConsumer(channel);
                channel.basicConsume(queueName, true, consumer);
                while (true) {
                    QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                    String message = new String(delivery.getBody());
                    LOGGER.log(message, logLevel);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
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
                for (String level : LEVELS) {
                    try {
                        channel.basicPublish(EXCHANGE_NAME, level, null, message.getBytes());
                        LOGGER.log(format("[%s]Sent: %s", level, message), "SENDER");
                    } catch (IOException e) {
                        LOGGER.log(format("[%s]Sent: %s", level, message), "SENDER");
                        e.printStackTrace();
                    }
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
