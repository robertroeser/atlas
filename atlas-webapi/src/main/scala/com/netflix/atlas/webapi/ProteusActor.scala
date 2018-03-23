package com.netflix.atlas.webapi

import java.time.Duration
import java.util.function.Consumer

import akka.actor.{Actor, ActorLogging}
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.validation.{Rule, ValidationResult}
import com.netflix.atlas.webapi.PublishApi.PublishRequest
import io.netifi.proteus.metrics.om._
import io.netty.buffer.ByteBuf
import io.netty.channel.nio.NioEventLoopGroup
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.{ConnectionSetupPayload, RSocket, RSocketFactory, SocketAcceptor}
import org.reactivestreams.Publisher
import reactor.core.publisher.{Flux, Mono}
import reactor.ipc.netty.options.ServerOptions
import reactor.ipc.netty.tcp.TcpServer

class ProteusActor extends Actor with ActorLogging with MetricsSnapshotHandler {
  private val dbRef = context.actorSelection("/user/publish")

  private val rules = ApiSettings.validationRules

  val handler: RSocket = new MetricsSnapshotHandlerServer(this)
  val tcpServer = TcpServer.builder()
    .options(new Consumer[ServerOptions.Builder[_ <: ServerOptions.Builder[_]]] {
      override def accept(options: ServerOptions.Builder[_ <: ServerOptions.Builder[_]]): Unit = {
        options.eventLoopGroup(new NioEventLoopGroup()(0, context.dispatcher))
        options.port(9800)
      }
    }).build()

  val server = RSocketFactory.receive()
    .acceptor(new SocketAcceptor {
      override def accept(setup: ConnectionSetupPayload, sendingSocket: RSocket) = Mono.just(handler)
    })
    .transport(TcpServerTransport.create(tcpServer))
    .start()
    .block()


  override def receive: Receive = {
    case _ =>
  }

  override def streamMetrics(publisher: Publisher[MetricsSnapshot], byteBuf: ByteBuf): Flux[Skew] = {
    val disposable = Flux
      .from(publisher)
      .flatMap(snapshot => {
        val commonTags = snapshot.getTagsMap
        val metrics = snapshot.getMetricsList

        var tags = Map[String, String]()
        commonTags
          .forEach((k, v) => {
            tags += (k -> v)
          })

        Flux
          .fromIterable(metrics)
          .map(_ => convert(_, tags))
      })
      .subscribe()

    Flux
      .interval(Duration.ofSeconds(30))
      .map(_ =>
        Skew
          .newBuilder()
          .setTimestamp(System.currentTimeMillis())
          .build())
      .doFinally(_ => disposable.dispose())
  }

  private def convert(meterMeasurement: MeterMeasurement, commonTags: Map[String, String]): Datapoint = {
    var tags = commonTags
    val id = meterMeasurement.getId
    tags += "name" -> id.getName

    id.getTagList
      .forEach(t => {
        tags += t.getKey -> t.getValue
      })

    new Datapoint(tags, meterMeasurement.getTimestamp, meterMeasurement.getValue)
  }

  private def validate(vs: List[Datapoint]): PublishRequest = {
    val validDatapoints = List.newBuilder[Datapoint]
    val failures = List.newBuilder[ValidationResult]
    val now = System.currentTimeMillis()
    val limit = ApiSettings.maxDatapointAge
    vs.foreach { v =>
      val diff = now - v.timestamp
      val result = diff match {
        case d if d > limit =>
          val msg = s"data is too old: now = $now, timestamp = ${v.timestamp}, $d > $limit"
          ValidationResult.Fail("DataTooOld", msg)
        case d if d < -limit =>
          val msg = s"data is from future: now = $now, timestamp = ${v.timestamp}"
          ValidationResult.Fail("DataFromFuture", msg)
        case _ =>
          Rule.validate(v.tags, rules)
      }
      if (result.isSuccess) validDatapoints += v else failures += result
    }
    PublishRequest(validDatapoints.result(), failures.result())
  }
}
