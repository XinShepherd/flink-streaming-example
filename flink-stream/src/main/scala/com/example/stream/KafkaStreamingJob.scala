package com.example.stream

import java.util.Properties

import com.example.stream.util.Json
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import com.example.stream.util.Json

/**
  * Created on 1/10/2019
  *
  * @author Shepherd
  */
object KafkaStreamingJob extends App {

  override def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.10.17.95:9092")
    properties.setProperty("group.id", "test")
    val myConsumer = new FlinkKafkaConsumer010[String](
      "flight_verify_price",
      new SimpleStringSchema,
      properties)

    val stream = env.addSource(myConsumer)
    stream.print()

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.0.105", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,
      new ElasticsearchSinkFunction[String] {
        override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          val sourceNode = Json.parse(element)
          val json = new java.util.HashMap[String, Any]
          json.put("data", sourceNode.get("@content").get("request").toString)
          json.put("timestamp", sourceNode.get("@start").asLong())
          val indexReq = Requests.indexRequest()
            .index("flink-test")
            .`type`("_doc")
            .source(json)
          indexer.add(indexReq)
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    // provide a RestClientFactory for custom configuration on the internally created REST client
//    esSinkBuilder.setRestClientFactory(
//      restClientBuilder => {
//        restClientBuilder.setDefaultHeaders(...)
//        restClientBuilder.setMaxRetryTimeoutMillis(...)
//        restClientBuilder.setPathPrefix(...)
//        restClientBuilder.setHttpClientConfigCallback(...)
//      }
//    )

    // finally, build and add the sink to the job's pipeline
    stream.addSink(esSinkBuilder.build)

    env.execute("Kafka Streaming Job.")
  }
}
