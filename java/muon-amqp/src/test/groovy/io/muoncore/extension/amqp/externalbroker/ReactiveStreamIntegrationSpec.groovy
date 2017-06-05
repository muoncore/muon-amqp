package io.muoncore.extension.amqp.externalbroker

import io.muoncore.MultiTransportMuon
import io.muoncore.Muon
import io.muoncore.channel.impl.StandardAsyncChannel
import io.muoncore.codec.json.JsonOnlyCodecs
import io.muoncore.config.AutoConfiguration
import io.muoncore.extension.amqp.AMQPMuonTransport
import io.muoncore.extension.amqp.BaseEmbeddedBrokerSpec
import io.muoncore.extension.amqp.DefaultAmqpChannelFactory
import io.muoncore.extension.amqp.DefaultServiceQueue
import io.muoncore.extension.amqp.MyTestClass
import io.muoncore.extension.amqp.rabbitmq09.RabbitMq09ClientAmqpConnection
import io.muoncore.extension.amqp.rabbitmq09.RabbitMq09QueueListenerFactory
import io.muoncore.memory.discovery.InMemDiscovery
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.Environment
import reactor.fn.BiConsumer
import reactor.rx.broadcast.Broadcaster
import spock.lang.*
import spock.util.concurrent.PollingConditions

import static io.muoncore.codec.types.MuonCodecTypes.listOf

@Ignore
class ReactiveStreamIntegrationSpec extends BaseEmbeddedBrokerSpec {

    @Shared def discovery = new InMemDiscovery()

    @AutoCleanup("shutdown")
    @Shared
    def muon1

    @AutoCleanup("shutdown")
    @Shared def muon2

    def setupSpec() {
      println "Starting test ... "
      muon1 = muon("simples")
      muon2 = muon("tombola")
    }

    def "can create a publisher and subscribe to it remotely"() {

        StandardAsyncChannel.echoOut = true
        def env = Environment.initializeIfEmpty()

        def data = []

        def b = Broadcaster.create(env)
        def sub2 = Broadcaster.create(env)

        sub2.consume {
            data << it
        }

        muon1.publishSource("somedata", PublisherLookup.PublisherType.HOT, b)

        sleep(4000)
        when:
        muon2.subscribe(new URI("stream://simples/somedata"), sub2)

        sleep(1000)

        and:
        20.times {
            println "Publish"
            b.accept(["hello": "world"])
        }
        sleep(5000)

        then:

//        new PollingConditions(timeout: 20).eventually {
            data.size() == 20
//        }

        cleanup:
        StandardAsyncChannel.echoOut = false
    }

    def "can create a publisher and subscribe to it remotely many times"() {

        StandardAsyncChannel.echoOut = true
        def env = Environment.initializeIfEmpty()

        def numTimes = 20
        def numMessages = 200
        def data = []

        def b = Broadcaster.create(env)
        b.consume {
            println "Received data ${it}"
        }
        b.observeError(Exception, new BiConsumer() {
            @Override
            void accept(Object o, Object o2) {
                println "Something bad happend"
            }
        })

        muon1.publishSource("somedata", PublisherLookup.PublisherType.HOT, b)

        sleep(6000)
        when:

        numTimes.times { val ->
            def sub2 = Broadcaster.create(env)

            sub2.consume {
                println "[${val}]REMOTE Receive ${it}"
                data << it
            }
            sub2.observeError(Exception, new BiConsumer() {
                @Override
                void accept(Object o, Object o2) {
                    println "Terrible things. very very bad....."
                }
            })
            sub2.observeCancel({
                println "The stream was cancelled"
            })
            sub2.observeComplete({
                println "The stream is completed"
            })
            muon2.subscribe(new URI("stream://simples/somedata"), sub2)
        }

        def tapper = Broadcaster.create(env)
        tapper.consume {
            println "Got a transport subscriber ${it.id}... "
        }

//        muon2.transportControl.tap({
//            it.type == "SubscriptionRequested"
//        }).subscribe(tapper)

        sleep(5000)

        and:
        numMessages.times {
            println "Publish"
            b.accept(["hello": "world ${it}".toString()])
            sleep(100)
        }
        sleep(5000)

        then:

        new PollingConditions(timeout: 20).eventually {
            data.size() == numMessages * numTimes
        }

        cleanup:
        StandardAsyncChannel.echoOut = false
    }

    def "data remains in order"() {

        StandardAsyncChannel.echoOut = true
        def env = Environment.initializeIfEmpty()

        def data = []

        def b = Broadcaster.create(env)
        def sub2 = Broadcaster.create(env)

        sub2.consume {
            data << it
        }

        muon1.publishSource("somedata", PublisherLookup.PublisherType.HOT, b)

        sleep(4000)
        when:
        muon2.subscribe(new URI("stream://simples/somedata"), sub2)

        sleep(1000)

        def inc = 1

        and:
        200.times {
            println "Publish"
            b.accept(inc++)
        }
        sleep(5000)

        def sorted = new ArrayList<>(data).sort()

        then:

        data == sorted

        cleanup:
        StandardAsyncChannel.echoOut = false
    }

    def "can subscribe to events, which are lists"() {

        StandardAsyncChannel.echoOut = true
        def env = Environment.initializeIfEmpty()

        def data

        def b = Broadcaster.create(env)
        def sub2 = Broadcaster.create(env)

        sub2.consume {
            if (data == null) {
                data = it.getPayload(listOf(MyTestClass))
            } else {
                throw new IllegalStateException()
            }
        }

        muon1.publishSource("somedata", PublisherLookup.PublisherType.HOT, b)

        sleep(4000)
        when:
        muon2.subscribe(new URI("stream://simples/somedata"), sub2)

        sleep(1000)

        and:
        List<MyTestClass> event = [new MyTestClass(someValue: "test1", someOtherValue: 1),
                                   new MyTestClass(someValue: "test2", someOtherValue: 2)]

        b.accept(event)
        sleep(5000)

        then:

//        new PollingConditions(timeout: 20).eventually {
        data == event
//        }

        cleanup:
        StandardAsyncChannel.echoOut = false
    }

//    @Ignore
    def "subscribing to remote fails with onError"() {

        def data = []
        def errorReceived = false

        def sub2 = new Subscriber() {
          @Override
          void onSubscribe(Subscription s) {}

          @Override
          void onNext(Object o) {}

          @Override
          void onError(Throwable t) { errorReceived = true}

          @Override
          void onComplete() {}
        }


        def muon1 = muon("simples")
        def muon2 = muon("tombola")

        when:
        muon2.subscribe(new URI("stream://simples/BADSTREAM"), sub2)

        then:
        new PollingConditions().eventually {
            errorReceived
        }
    }

    private Muon muon(serviceName) {

        def connection = new RabbitMq09ClientAmqpConnection("amqp://muon:microservices@localhost:6743")
        def queueFactory = new RabbitMq09QueueListenerFactory(connection.channel)
        def serviceQueue = new DefaultServiceQueue(serviceName, connection)
        def channelFactory = new DefaultAmqpChannelFactory(serviceName, queueFactory, connection)

        def svc1 = new AMQPMuonTransport(
                "amqp://muon:microservices@localhost:6743", serviceQueue, channelFactory)

        def config = new AutoConfiguration(serviceName:serviceName)
        def muon = new MultiTransportMuon(config, discovery, [svc1], new JsonOnlyCodecs())

        muon
    }
}
