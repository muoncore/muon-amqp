package io.muoncore.extension.amqp;

import com.rabbitmq.client.AlreadyClosedException;
import io.muoncore.Discovery;
import io.muoncore.channel.impl.StandardAsyncChannel;
import io.muoncore.channel.support.Scheduler;
import io.muoncore.codec.Codecs;
import io.muoncore.exception.MuonException;
import io.muoncore.exception.MuonTransportFailureException;
import io.muoncore.message.MuonInboundMessage;
import io.muoncore.message.MuonMessage;
import io.muoncore.message.MuonMessageBuilder;
import io.muoncore.message.MuonOutboundMessage;
import io.muoncore.transport.TransportEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Dispatcher;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DefaultAmqpChannel implements AmqpChannel {

    private String sendQueue;
    private String receiveQueue;
    private QueueListenerFactory listenerFactory;
    private ChannelFunction<MuonInboundMessage> function;
    private AmqpConnection connection;
    private String localServiceName;
    private Logger log = LoggerFactory.getLogger(DefaultAmqpChannel.class.getName());
    private Dispatcher dispatcher;
    private Codecs codecs;
    private Discovery discovery;
    private Scheduler scheduler;
    private boolean channelLive = true;

    private CountDownLatch handshakeControl = new CountDownLatch(1);

    private QueueListener listener;

    private ChannelFunction onShutdown;

    private boolean ownsQueues = false;

    public DefaultAmqpChannel(AmqpConnection connection,
                              QueueListenerFactory queueListenerFactory,
                              String localServiceName, Dispatcher dispatcher,
                              Codecs codecs,
                              Discovery discovery,
                              Scheduler scheduler) {
        this.connection = connection;
        this.listenerFactory = queueListenerFactory;
        this.localServiceName = localServiceName;
        this.dispatcher = dispatcher;
        this.codecs = codecs;
        this.discovery = discovery;
        this.scheduler = scheduler;
    }

    @Override
    public void onShutdown(ChannelFunction runnable) {
        this.onShutdown = runnable;
    }

    @Override
    public void initiateHandshake(String serviceName, String protocol) {
        ownsQueues = true;
        receiveQueue = localServiceName + "-receive-" + UUID.randomUUID().toString();
        sendQueue = serviceName + "-receive-" + UUID.randomUUID().toString();

        listener = listenerFactory.listenOnQueue(receiveQueue, msg -> {
            if (msg == null) {
                shutdown();
                return;
            }

            log.trace("Received a message on the receive queue " + msg.getQueueName());
            if ("accepted".equals(msg.getHandshakeMessage())) {
                log.trace("Handshake completed");
                handshakeControl.countDown();
                return;
            }
            if (function != null) {
                MuonInboundMessage inbound = AmqpMessageTransformers.queueToInbound(msg, codecs);
                if (inbound.getStep().equals(TransportEvents.CONNECTION_FAILURE)) {
                    function.apply(null);
                } else {
                    dispatcher.dispatch(inbound,
                            function::apply, Throwable::printStackTrace);
                }
            }
        }, this::shutdown);

        try {
            connection.send(QueueMessageBuilder.queue("service." + serviceName)
                    .protocol(protocol)
                    .serverReplyTo(receiveQueue)
                    .handshakeMessage("initiated")
                    .recieveQueue(sendQueue)
                    .build());
        } catch (IOException e) {
            throw new MuonTransportFailureException("Unable to initiate handshake", e);
        }
        try {
            if (!handshakeControl.await(3000, TimeUnit.MILLISECONDS))
                throw new MuonException("The handshake took too long! target " + serviceName + " / " + protocol + " || " + receiveQueue);
        } catch (InterruptedException e) {
            throw new MuonException("The handshake took too long!", e);
        }
    }

    @Override
    public void respondToHandshake(AmqpHandshakeMessage message) {
        ownsQueues = false;
        log.debug("Handshake received " + message);
        receiveQueue = message.getReceiveQueue();
        sendQueue = message.getReplyQueue();
        log.debug("Opening queue to listen " + receiveQueue);
        listener = listenerFactory.listenOnQueue(receiveQueue, msg -> {
            if (msg == null) {
                shutdown();
                return;
            }
            MuonInboundMessage inboundMessage = AmqpMessageTransformers.queueToInbound(msg, codecs);
            log.debug("Received inbound channel message [" + receiveQueue + "] of type " + message.getProtocol() + ":" + inboundMessage.getStep());
            if (StandardAsyncChannel.echoOut) System.out.println(new Date() + ": Channel[ AMQP Wire >>>>> DefaultAMQPChannel]: Received " + inboundMessage);
            if (inboundMessage.getChannelOperation() == MuonMessage.ChannelOperation.closed) {
                function.apply(null);
            } else if (function != null) {
                function.apply(inboundMessage);
            }
        }, this::shutdown);

        try {
            QueueListener.QueueMessage handshakeResponse = QueueMessageBuilder.queue(message.getReplyQueue())
                    .protocol(message.getProtocol())
                    .handshakeMessage("accepted").build();
            log.debug("Handshake Response sent " + handshakeResponse);
            connection.send(handshakeResponse);

        } catch (IOException e) {
            throw new MuonTransportFailureException("Unable to respond to handshake", e);
        } finally {
            handshakeControl.countDown();
        }
    }

    @Override
    public void shutdown() {
        if (channelLive) {
            log.info("Sending shutdown message to client side channel");
            function.apply(MuonMessageBuilder.fromService(localServiceName)
                    .step(TransportEvents.CONNECTION_FAILURE)
                    .operation(MuonMessage.ChannelOperation.closed)
                    .contentType("text/plain")
                    .payload(new byte[0])
                    .buildInbound());
//            function.apply(null);
            channelLive = false;
            try {
                listener.cancel();
                connection.deleteQueue(sendQueue);
                connection.deleteQueue(receiveQueue);
            } catch (Throwable e) {
                log.error("Error cleaning up AMQP Channel {}", e.getMessage());
            }
        }
        if (onShutdown != null) {
            this.onShutdown.apply(null);
        }
    }

    @Override
    public void receive(ChannelFunction<MuonInboundMessage> function) {
        this.function = function;
    }

    @Override
    public void send(MuonOutboundMessage message) {
        if (!channelLive) {
            if (StandardAsyncChannel.echoOut) System.out.println(new Date() + ": Channel[ DefaultAMQPChannel ]: CHANNEL CLOSED  " + message);
            return;
        }

        if (StandardAsyncChannel.echoOut) System.out.println(new Date() + ": Channel[ DefaultAMQPChannel >>>>> AMQP Wire]: Sending " + message);
        if (message != null) {
            log.debug("Sending inbound channel message of type " + message.getProtocol() + "||" + message.getStep());
            dispatcher.dispatch(message, msg -> {
                try {
                    connection.send(AmqpMessageTransformers.outboundToQueue(sendQueue, message, codecs, discovery));
                    if (msg.getChannelOperation() == MuonMessage.ChannelOperation.closed) {
                      shutdown();
                    }
                } catch (IOException | AlreadyClosedException e) {
                    log.warn("Connection Error sending to AMQP Broker, killing channel {}", e.getMessage());
                    shutdown();
                }
            }, Throwable::printStackTrace);
        } else {
            send(
                    MuonMessageBuilder.fromService(localServiceName)
                            .step(TransportEvents.CONNECTION_FAILURE)
                            .operation(MuonMessage.ChannelOperation.closed)
                            .contentType("text/plain")
                            .payload(new byte[0])
                            .build());


        }
    }
}
