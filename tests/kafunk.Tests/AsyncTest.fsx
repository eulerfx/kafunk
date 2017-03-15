#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

module Async =

  let withCancellation (ct:CancellationToken) (a:Async<'a>) : Async<'a> = async {
    let! ct' = Async.CancellationToken
    let cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct')
    return!
      Async.StartAsTask (a, cancellationToken=cts.Token)
      |> Async.awaitTaskCancellationAsError }

//  let withCancellation (ct:CancellationToken) (a:Async<'a>) : Async<'a> = async {
//    let! ct2 = Async.CancellationToken
//    use cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct2)
//    let res = IVar.create ()
//    use _reg = cts.Token.Register (fun () -> res.SetCanceled())
//    let a = async {
//      try
//        let! a = a
//        IVar.put a res
//      with ex ->
//        IVar.error ex res }
//    Async.Start (a, cts.Token)
//    return! res |> IVar.get }

let cts = new CancellationTokenSource()
let internal buf = BoundedMb.create 100

let writeProc = async {
  use! _cnc = Async.OnCancel (fun () -> printfn "cancelling_write_proc")
  printfn "running_proc"
  return!
    AsyncSeq.initInfinite id
    |> AsyncSeq.iterAsync (fun x -> async {
      printfn "producing=%i" x
      do! buf |> BoundedMb.put x
      do! Async.Sleep 1000 }) }

let readProc = async {
  //use! _cnc = Async.OnCancel (fun () -> printfn "cancelling_read_proc")
  let! r =
    AsyncSeq.replicateInfiniteAsync (BoundedMb.take buf |> Async.withCancellation cts.Token)
    //AsyncSeq.replicateInfiniteAsync (BoundedMb.take buf)
    |> AsyncSeq.iterAsync (fun x -> async {
      printfn "consuming=%i" x
      return () })
  return () }


let go = async {
  
  printfn "starting_read_proc"
  let rt = Async.StartAsTask readProc
  rt |> Task.extend (fun rt -> printfn "read_proc_task_finished=%A error=%O" rt.Status rt.Exception) |> ignore

  printfn "starting_write_proc"
  let proc = Async.withCancellation cts.Token writeProc
  let t = Async.StartAsTask (proc)
  let! r = Async.AwaitTask t |> Async.Catch
  printfn "write_proc_result=%A" r
  return () }

cts.CancelAfter 5000

Async.RunSynchronously (go)









