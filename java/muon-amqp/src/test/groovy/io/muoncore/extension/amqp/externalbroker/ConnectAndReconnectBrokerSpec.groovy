package io.muoncore.extension.amqp.externalbroker

import io.muoncore.InstanceDescriptor
import io.muoncore.channel.support.Scheduler
import io.muoncore.codec.json.JsonOnlyCodecs
import io.muoncore.extension.amqp.AMQPMuonTransport
import io.muoncore.extension.amqp.BaseEmbeddedBrokerSpec
import io.muoncore.extension.amqp.DefaultAmqpChannelFactory
import io.muoncore.extension.amqp.DefaultServiceQueue
import io.muoncore.extension.amqp.rabbitmq09.RabbitMq09ClientAmqpConnection
import io.muoncore.extension.amqp.rabbitmq09.RabbitMq09QueueListenerFactory
import io.muoncore.memory.discovery.InMemDiscovery
import io.muoncore.protocol.DynamicRegistrationServerStacks
import io.muoncore.protocol.ServerStacks
import io.muoncore.protocol.defaultproto.DefaultServerProtocol
import io.muoncore.transport.client.SimpleTransportMessageDispatcher
import reactor.Environment
import spock.lang.AutoCleanup
import spock.lang.Ignore
import spock.lang.Shared
import spock.util.concurrent.PollingConditions

@Ignore
class ConnectAndReconnectBrokerSpec extends BaseEmbeddedBrokerSpec {

  @Shared
  def discovery = new InMemDiscovery()

  @AutoCleanup("shutdown")
  @Shared
  AMQPMuonTransport transport

  @Shared
  ServerStacks serverStacks = new DynamicRegistrationServerStacks(new DefaultServerProtocol(null, null, null), new SimpleTransportMessageDispatcher())

  def setupSpec() {
    transport = muon("simples")
    discovery.advertiseLocalService(new InstanceDescriptor("123", "simples", [], ["application/json"], [new URI("amqp://muon:microservices@localhost:6743")], []))
  }

  def "will reconnect to a broker after connecting and the broker failing"() {
    given: "An open connection to a broker"
    def env = Environment.initializeIfEmpty()

    when: "The broker fails"
    rabbitMq.stop()
    Thread.sleep(5000)

    and: "The broker restarts"
    rabbitMq.start()

    then: "The transport will reconnect"

    new PollingConditions(timeout: 30).eventually {
      try {
        transport.openClientChannel("simples", "rpc")
      } catch(Exception e) {
        println "Failed to connect ... ${e.message}"
        false
      }
    }

  }

  private AMQPMuonTransport muon(serviceName) {

    def connection = new RabbitMq09ClientAmqpConnection("amqp://muon:microservices@localhost:6743")
    def serviceQueue = new DefaultServiceQueue(serviceName, connection)
    def channelFactory = new DefaultAmqpChannelFactory(serviceName, new RabbitMq09QueueListenerFactory(connection.getChannel()), connection)

    def ret =  new AMQPMuonTransport(
      "amqp://muon:microservices@localhost:6743", serviceQueue, channelFactory)
    ret.start(discovery, serverStacks, new JsonOnlyCodecs(), new Scheduler())
    return ret
  }

}
