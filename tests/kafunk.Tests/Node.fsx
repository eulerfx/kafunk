#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

(*

# Server vs Computation

- A server is never expected to finish whereas a computation has an explicit timeout
- A server never produces a value, but may fail.
- Can the computation which starts a node complete succesfully, leaving the node completed, but unfaulted? Is this a well-defined behavior?
- When to start a node vs just a computation?
- Should a node be decoupled from a generation of node state? 
  - A node has a single node state at a point in time, but many during its lifecycle.
  - Operations can be bound to a specific node state, and for example, ignored by future node states.

Node vs NodeState?


Group 
  - Heartbeat
  - Consumer
    - Fetcher

Producer


Projector 
  - Monitoring




*)

/// A node defines a starting point for async computations.
/// Computations started from a node are bidirectionally linked to the node
/// such that if the computation fails, the node is failed, which in turn,
/// causes all other computations linked to the node to fail.
type Node = private {
  node : IVar<unit>
  cancellation : CancellationToken }

[<Compile(Module)>]
module Node =

  /// Creates a new node.
  let node () = 
    let iv = IVar.create ()
    let cts = new CancellationTokenSource()
    iv.Task |> Task.extend (fun _ -> cts.Cancel ()) |> ignore
    { node = iv ; cancellation = cts.Token }

  /// Starts an async computation as a node.
  /// The node completes when the async computation completes.
  let startNodeAsync (a:Async<unit>) : Node =
    let p = node ()
    Async.startThreadPoolWithContinuations (
      a, 
      (fun () -> IVar.tryPut () p.node |> ignore), 
      (fun e -> IVar.tryError e p.node |> ignore), 
      ignore, 
      p.cancellation)
    p

  /// Starts an async computation as a node.
  /// The node completes when the async computation completes.
  let startNodeChildAsync (a:Async<unit>) : Async<Node> = async {
    let! ct = Async.CancellationToken
    let n = node ()
    let cts = CancellationTokenSource.CreateLinkedTokenSource (ct, n.cancellation)
    Async.startThreadPoolWithContinuations (
      a, 
      (fun () -> IVar.tryPut () n.node |> ignore), 
      (fun e -> IVar.tryError e n.node |> ignore), 
      ignore, 
      cts.Token)
    return n }

  /// Returns a Task that faults only if the node errors.
  let errorTask (n:Node) : Task<'a> =
    Task.taskFault n.node.Task

  /// Marks the node as completed with fault.
  let error (n:Node) (e:exn) =
    IVar.tryError e n.node |> ignore

  let isComplete (n:Node) =
    n.node.Task.IsCompleted

  /// Marks the node as completed without fault.
  let complete (n:Node) =
    IVar.tryPut () n.node |> ignore

  /// Returns an async computation which completes when the node completes.
  let join (n:Node) : Async<unit> =
    IVar.get n.node

  let private linkTaskError (n:Node) (t:Task<_>) =
    t 
    |> Task.extend (fun t -> 
      if t.IsFaulted then error n t.Exception
      else ())
    |> ignore

  let startChildAsyncAsTask (n:Node) (child:Async<'a>) : Task<'a> =
    let t = Async.StartAsTask (child, cancellationToken = n.cancellation)
    linkTaskError n t
    t

  let link (parent:Node) (child:Node) =
    parent.node.Task 
    |> Task.extend (fun t ->
      if t.IsCanceled then IVar.tryCancel child.node
      elif t.IsFaulted then IVar.tryError t.Exception child.node
      else IVar.tryPut t.Result child.node)
    |> ignore

  let private chooseTaskAsTask (t:Task<'a>) (a:Async<'a>) = async {
    let! a = Async.StartChildAsTask a
    return Task.WhenAny (t, a) |> Task.join }

  let private chooseTask (t:Task<'a>) (a:Async<'a>) : Async<'a> =
    chooseTaskAsTask t a |> Async.bind Async.AwaitTask

  /// Returns an async computation which runs the argument computation
  /// but if the node fails, escalates the failure.
  let tryAsync (n:Node) (a:Async<'a>) : Async<'a> =
    chooseTask (errorTask n) a


/// A node-dependent computation.
type Proc<'a> = 
  private
  | P of (Node -> Async<'a>)

module Proc =
  
  /// Runs the process on the specified node.
  let run (P(f)) n = f n

  /// Returns a process which evaluates the specified async computation
  /// in the context of the node on which the process is run.
  let liftAsync (a:Async<'a>) : Proc<'a> =
    P (fun n -> Node.tryAsync n a)

  let puree (a:'a) = P (fun _ -> async.Return a)

  let map (f:'a -> 'b) (p:Proc<'a>) : Proc<'b> =
    P (fun n -> async {
        let! a = run p n
        return f a })

  let bind (f:'a -> Proc<'b>) (p:Proc<'a>) : Proc<'b> =
    P (fun n -> async {
        let! a = run p n
        let! b = run (f a) n
        return b })

  let bindAsync (f:'a -> Proc<'b>) (p:Async<'a>) : Proc<'b> =
    bind f (liftAsync p)

  /// Returns the node on which the process is running.
  let node : Proc<Node> =
    P (fun n -> async { return n })

  let fail (ex:exn) : Proc<unit> =
    P (fun n -> async {
        Node.error n ex
        return () })

  let startChildAsTask (a:Async<'a>) : Proc<Task<'a>> =
    P (fun n -> async {
        return Node.startChildAsyncAsTask n a })

  type Builder () =
    member __.Return a = puree a
    member __.Bind (p:Proc<'a>, f:'a -> Proc<'b>) : Proc<'b> = bind f p
    member __.Bind (a:Async<'a>, f:'a -> Proc<'b>) : Proc<'b> = bindAsync f a

/// A workflow builder for a node-dependent computation.
let proc = Proc.Builder ()


let go (a:Async<'a>) = proc {
  
  // evaluates an async computation in the context
  // of the ambient node
  let! a = a

  // starts a linked async computation
  let! t = Proc.startChildAsTask (async { printfn "hello" })

  let! node = Proc.node

  do! Proc.fail (exn("oh no"))

  return 1 }

    






module AsyncSeq =

  let mapAsyncParallel (f:'a -> Async<'b>) (s:AsyncSeq<'a>) : AsyncSeq<'b> = asyncSeq {
    use mb = Mb.create ()
    let! err =
      s 
      |> AsyncSeq.iterAsync (fun a -> async {
        let! b = Async.StartChild (f a)
        mb.Post (Some b) })
      |> Async.map (fun _ -> mb.Post None)
      |> Node.startNodeChildAsync
    yield! 
      AsyncSeq.replicateUntilNoneAsync (Node.tryAsync err (Mb.take mb))
      |> AsyncSeq.mapAsync id }