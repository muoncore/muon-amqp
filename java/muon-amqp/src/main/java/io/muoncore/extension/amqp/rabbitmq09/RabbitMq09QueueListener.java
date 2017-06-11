package io.muoncore.extension.amqp.rabbitmq09;


import com.rabbitmq.client.*;
import io.muoncore.channel.ChannelConnection;
import io.muoncore.extension.amqp.AmqpConnection;
import io.muoncore.extension.amqp.QueueListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class RabbitMq09QueueListener implements QueueListener {

    private boolean running;
    private AmqpConnection.ExecuteWithChannel channel;
    private Logger log = LoggerFactory.getLogger(RabbitMq09QueueListener.class.getName());
    private String queueName;
    private QueueListener.QueueFunction listener;
    private Consumer consumer;
    private CountDownLatch latch = new CountDownLatch(1);
    private Runnable onShutdown;

    public RabbitMq09QueueListener(AmqpConnection.ExecuteWithChannel channel, String queueName, QueueListener.QueueFunction function, Runnable onShutdown) {
        this.channel = channel;
        this.queueName = queueName;
        this.listener = function;
        this.onShutdown = onShutdown;
    }

    public void blockUntilReady() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Error waiting for amqp listener to start", e);
        }
    }

    public void start() {
        run();
    }

    public void run() {
        channel.executeOnEveryConnect(channel -> {
            try {
                log.debug("Opening Queue: " + queueName);
                channel.queueDeclare(queueName, false, false, true, null);
                channel.addShutdownListener(cause -> {
                    log.error("AMQP Channel is shut down by its connnection {}", cause.getMessage());
                    onShutdown.run();
                });
                consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        try {

                            Map<String, Object> headers = properties.getHeaders();

                            if (headers == null) {
                                headers = new HashMap<>();
                            }

                            Map<String, String> newHeaders = new HashMap<>();
                            headers.entrySet().stream().forEach(entry -> {
                                if (entry.getKey() == null || entry.getValue() == null) {
                                    return;
                                }
                                newHeaders.put(entry.getKey(), entry.getValue().toString());
                            });

                            log.debug("Receiving message on " + queueName + " of type " + newHeaders.get("eventType"));

                            listener.exec(new QueueListener.QueueMessage(queueName, body, newHeaders));

                            channel.basicAck(envelope.getDeliveryTag(), false);
                        } catch (ShutdownSignalException | ConsumerCancelledException ex) {
                            log.warn("FAILED!" + ex.getMessage(), ex);
                        } catch (Exception e) {
                            log.warn(e.getMessage(), e);
                        }
                    }
                };

                channel.basicConsume(queueName, false, consumer);

                latch.countDown();

                log.debug("Queue ready: " + queueName);

            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        });
    }

    public void cancel() {
        log.debug("Queue listener is cancelled:" + queueName);
        running = false;
        listener.exec(null);
        try {
            consumer.handleCancel("Muon-Cancel");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            channel.executeNowIfChannelIsOpen(channel -> {
                try {
                    channel.queueDelete(queueName, false, false);

                } catch (IOException | AlreadyClosedException e) {
                    log.warn("Error while cancelling listener, {}", e.getMessage());
                }
            });

        }
    }
}
