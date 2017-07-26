package io.bfil.kafka

import java.time.Duration
import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.collection.mutable
import kamon.Kamon
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object Main extends App {

  Kamon.start()

  BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.WARN)

  val config = ConfigFactory.load()

  val connectionProperties = new Properties()
  connectionProperties.put("bootstrap.servers", config.getString("kafka.bootstrap-servers"))

  if(config.getString("kafka.security-protocol") == "SSL") {
    connectionProperties.put("security.protocol", config.getString("kafka.security-protocol"))
    connectionProperties.put("ssl.protocol", config.getString("kafka.ssl-protocol"))
    connectionProperties.put("ssl.key.password", config.getString("kafka.ssl-key-password"))
    connectionProperties.put("ssl.keystore.location", config.getString("kafka.ssl-keystore-location"))
    connectionProperties.put("ssl.keystore.password", config.getString("kafka.ssl-keystore-password"))
    connectionProperties.put("ssl.keystore.type", config.getString("kafka.ssl-keystore-type"))
    connectionProperties.put("ssl.truststore.location", config.getString("kafka.ssl-truststore-location"))
    connectionProperties.put("ssl.truststore.password", config.getString("kafka.ssl-truststore-password"))
    connectionProperties.put("ssl.truststore.type", config.getString("kafka.ssl-truststore-type"))
  }

  new KamonLagMonitor(
    connectionProperties = connectionProperties,
    consumerGroups = config.getStringList("kafka.lag-monitor.consumer-groups").asScala,
    groupId = config.getString("kafka.lag-monitor.group-id"),
    clientId = config.getString("kafka.lag-monitor.client-id"),
    pollInterval = config.getDuration("kafka.lag-monitor.poll-interval")
  ).run()

}

class KamonLagMonitor(connectionProperties: Properties, consumerGroups: Seq[String], groupId: String, clientId: String, pollInterval: Duration) {

  private val log = LoggerFactory.getLogger(classOf[KamonLagMonitor])

  private val consumerGroupService = new ConsumerGroupService(connectionProperties, groupId, clientId)

  private def offsetLagHistogram(group: String, state: String, clientId: String, topic: String, partition: Long) =
    Kamon.metrics.histogram(
      name = "kafka-consumer-groups-lag",
      tags = Map (
        "consumer-group"       -> group,
        "consumer-group-state" -> state,
        "clientId"             -> clientId,
        "topic"                -> topic,
        "partition"            -> partition.toString
      )
    )

  def run() =
    new Thread(() => while(true) {
      try {
        consumerGroups foreach { group =>
          val (groupState, assignmentStates) = consumerGroupService.collectGroupAssignment(group)
          assignmentStates.sortBy(s => (s.clientId, s.topic, s.partition)) foreach { state =>
            val histogram = offsetLagHistogram(group, groupState, state.clientId, state.topic, state.partition)
            state.lag match {
              case Some(lag) => histogram.record(lag)
              case None      => // Track nothing
            }
          }
        }
      } catch {
        case NonFatal(ex) => log.error(ex.getMessage, ex)
      }
      Thread.sleep(pollInterval.toMillis)
    }).start()

}

class ConsumerGroupService(connectionProperties: Properties, groupId: String, clientId: String) {

  private val adminClient = AdminClient.create(connectionProperties)

  private val consumer = {
    val properties = new Properties()
    val deserializer = (new StringDeserializer).getClass.getName
    properties.putAll(connectionProperties)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
    new KafkaConsumer(properties)
  }

  def listGroups() = adminClient.listAllConsumerGroupsFlattened().map(_.groupId)

  def collectGroupAssignment(group: String): (String, Seq[PartitionAssignmentState]) = {
    val consumerGroupSummary = adminClient.describeConsumerGroup(group)
    (consumerGroupSummary.state, consumerGroupSummary.consumers match {
      case None => Seq.empty
      case Some(consumers) =>
        var assignedTopicPartitions = Seq.empty[TopicPartition]
        val offsets = adminClient.listGroupOffsets(group)
        val rowsWithConsumer =
          if (offsets.isEmpty) List.empty
          else consumers.sortWith(_.assignment.size > _.assignment.size).flatMap { consumerSummary =>
            assignedTopicPartitions = assignedTopicPartitions ++ consumerSummary.assignment
            val partitionOffsets = consumerSummary.assignment.map { topicPartition =>
              topicPartition -> offsets.get(topicPartition)
            }.toMap
            collectConsumerAssignment(consumerSummary.assignment, partitionOffsets, s"${consumerSummary.host}", s"${consumerSummary.clientId}")
          }
        val rowsWithoutConsumer =
          offsets.filterNot {
            case (topicPartition, _) => assignedTopicPartitions.contains(topicPartition)
          }.flatMap {
            case (topicPartition, offset) => collectConsumerAssignment(Seq(topicPartition), Map(topicPartition -> Some(offset)), "No Host", "No Client ID")
          }
        rowsWithConsumer ++ rowsWithoutConsumer
    })
  }

  private def collectConsumerAssignment(topicPartitions: Seq[TopicPartition], partitionOffsets: Map[TopicPartition, Option[Long]], host: String, clientId: String) =
    getEndOffsets(topicPartitions)
      .map { case (topicPartition, endOffset) =>
        val consumerGroupOffset = partitionOffsets(topicPartition)
        PartitionAssignmentState(host, clientId, topicPartition, consumerGroupOffset, endOffset, calculateLag(consumerGroupOffset, endOffset))
      }

  private def getEndOffsets(topicPartitions: Seq[TopicPartition]): mutable.Map[TopicPartition, java.lang.Long] =
    consumer.endOffsets(topicPartitions.asJava).asScala

  private def calculateLag(offset: Option[Long], endOffset: Long) =
    offset.filter(_ != -1).map(offset => endOffset - offset)

  def close() = {
    adminClient.close()
    consumer.close()
  }

}

case class PartitionAssignmentState(host: String, clientId: String, topicPartition: TopicPartition, offset: Option[Long], endOffset: Long, lag: Option[Long]) {
  val topic = topicPartition.topic
  val partition = topicPartition.partition
}