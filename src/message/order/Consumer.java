package message.order;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

import static java.lang.Integer.valueOf;
import static message.order.Publisher.DIRECT;
import static message.order.Publisher.NAME;
import static message.order.Publisher.SOURCES;

public class Consumer {

    public static void main(String[] args) throws IOException {
        int index = args.length > 0 ? valueOf(args[0]) % 2 : 0;
        receiveUpdates(SOURCES[index]);
    }

    private static void receiveUpdates(String source) throws IOException {
        Channel channel = createChannel();
        channel.exchangeDeclare(NAME, DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, NAME, source);
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, false, consumer);
        long prevNumber = -1;
        while (true) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                System.out.println(message);
                long number = Long.valueOf(message);
                if (number < prevNumber) {
                    System.out.println("<<<<<<<<<< Out of Order >>>>>>>>>>>");
                }
                prevNumber = number;
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static Channel createChannel() throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        return connection.createChannel();
    }
}
