﻿#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System

//Log.MinLevel <- LogLevel.Trace
let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "existential-group"
let count = argiDefault 4 "1" |> Int32.Parse

let go = async {
  let! conn = 
    let connConfig = 
      let chanConfig = 
        ChanConfig.create (
          requestTimeout = TimeSpan.FromSeconds 20.0,
          receiveBufferSize = 8192 * 20,
          sendBufferSize = 8192 * 10,
          connectRetryPolicy = ChanConfig.DefaultConnectRetryPolicy,
          requestRetryPolicy = ChanConfig.DefaultRequestRetryPolicy)
      KafkaConfig.create (
        [KafkaUri.parse host], 
        tcpConfig = chanConfig,
        requestRetryPolicy = KafkaConfig.DefaultRequestRetryPolicy)
    Kafka.connAsync connConfig
  let consumerConfig = 
    ConsumerConfig.create (
      groupId = group, 
      topic = topic, 
      autoOffsetReset = AutoOffsetReset.StartFromTime Time.EarliestOffset,
      fetchMaxBytes = 5000000,
      fetchBufferSize = 2,
      sessionTimeout = 20000,
      heartbeatFrequency = 3,
      checkCrc = true)
  let! consumer = 
    Consumer.createAsync conn consumerConfig
  
  let showProgress =
    AsyncSeq.intervalMs 10000
    |> AsyncSeq.iterAsync (fun _ -> async {
      let! info = ConsumerInfo.consumerProgress consumer
      let str = 
        info.partitions
        |> Seq.map (fun p -> sprintf "[p=%i o=%i hwo=%i lag=%i lead=%i eo=%i mc=%i]" p.partition p.consumerOffset p.highWatermarkOffset p.lag p.lead p.earliestOffset p.messageCount)
        |> String.concat " ; "
      Log.info "consumer_progress|conn_id=%s topic=%s total_lag=%i min_lead=%i partitions=%s" conn.Config.connId info.topic info.totalLag info.minLead str
      return () })

  let! _ = Async.StartChild showProgress

  let handle (s:ConsumerState) (ms:ConsumerMessageSet) = async {
    use! _cnc = Async.OnCancel (fun () -> Log.warn "cancelling_handler")
    //do! Async.Sleep 30000
    Log.trace "consuming_message_set|topic=%s partition=%i count=%i size=%i first_offset=%i last_offset=%i high_watermark_offset=%i lag=%i"
      ms.topic
      ms.partition
      (ms.messageSet.messages.Length)
      (ConsumerMessageSet.size ms)
      (ConsumerMessageSet.firstOffset ms)
      (ConsumerMessageSet.lastOffset ms)
      (ms.highWatermarkOffset)
      (ConsumerMessageSet.lag ms) }

  use counter = Metrics.counter Log 5000

  let handle = 
    handle
    |> Metrics.throughputAsync2To counter (fun (_,ms,_) -> ms.messageSet.messages.Length)

  do! Consumer.consumePeriodicCommit consumer (TimeSpan.FromSeconds 10.0) handle
  //do! Consumer.stream consumer |> AsyncSeq.iterAsync (fun (s,ms) -> handle s ms)
//  do! 
//    Consumer.generations consumer
//    |> AsyncSeq.iterAsync (fun (s,ps) -> async {
//      Log.info "geneneration|generation_id=%i" s.generationId })
  //do! Async.never

}

Seq.init count (fun _ -> go)
|> Async.Parallel
|> Async.RunSynchronously
|> ignore