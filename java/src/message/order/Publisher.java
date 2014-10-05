package message.order;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.String.valueOf;

public class Publisher {

    private static final int PERIOD = 100;
    public static final String NAME = "nino";
    public static final String DIRECT = "direct";
    public static final String[] SOURCES = new String[]{"CNFS", "NWS"};

    public static void main(String[] args) throws IOException {
        sendUpdates();
    }

    private static void sendUpdates() throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(NAME, DIRECT);
        Timer ninoResponder = new Timer("Publisher");
        ninoResponder.schedule(new TimerTask() {
            @Override
            public void run() {
                long second = System.currentTimeMillis() / PERIOD;
                String message = valueOf(second);
                try {
                    String routingKey = SOURCES[((int) (second % 2))];
                    channel.basicPublish(NAME, routingKey, null, message.getBytes());
                    System.out.println("Sent: " + message);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, PERIOD);
    }
}
