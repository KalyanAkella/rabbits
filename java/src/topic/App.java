package topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.String.format;

public class App {
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String EXCHANGE_TYPE = "direct";
    private static final Logger LOGGER = new Logger();
    private static final String[] LEVELS = new String[] { "debug", "info", "warning", "error" };

    public static void main(String[] args) throws IOException {
        sendLogs();
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
