namespace Kafunk

//open System.Threading
//open System.Threading.Tasks
//
//type Proc = private {
//  proc : IVar<unit>
//  cancellation : CancellationToken }
//
//[<Compile(Module)>]
//module Proc =
//
//  /// Creates a new, empty process.
//  let proc () = 
//    let iv = IVar.create ()
//    let cts = new CancellationTokenSource()
//    iv.Task |> Task.extend (fun _ -> cts.Cancel ()) |> ignore
//    { proc = iv ; cancellation = cts.Token }
//
//  /// Returns a Task that completes only if the process errors.
//  let errorTask (p:Proc) : Task<'a> =
//    Task.taskFault p.proc.Task
//
//  let error (p:Proc) (e:exn) =
//    IVar.tryError e p.proc |> ignore
//
//  let isComplete (p:Proc) =
//    p.proc.Task.IsCompleted
//
//  let complete (p:Proc) =
//    IVar.tryPut () p.proc |> ignore
//
//  let join (p:Proc) : Async<unit> =
//    IVar.get p.proc
//
//  let private linkTaskError (p:Proc) (t:Task<_>) =
//    t 
//    |> Task.extend (fun t -> 
//      if t.IsFaulted then 
//        IVar.tryError t.Exception p.proc |> ignore
//      else ())
//    |> ignore
//
//  let startChildAsyncAsTask (p:Proc) (child:Async<'a>) : Task<'a> =
//    let t = Async.StartAsTask (child, cancellationToken = p.cancellation)
//    linkTaskError p t
//    t
//
////  let startChildAsync (p:Proc) (child:Async<'a>) : unit =
////    startChildAsyncAsTask p child |> ignore
//
//  /// Starts an async computation as a process.
//  /// The process completes when the async computation completes.
//  let startAsync (a:Async<unit>) : Proc =
//    let p = proc ()
//    Async.startThreadPoolWithContinuations (
//      a, 
//      (fun () -> IVar.tryPut () p.proc |> ignore), 
//      (fun e -> IVar.tryError e p.proc |> ignore), 
//      ignore, 
//      p.cancellation)
//    p
//
//  /// Starts an async computation as a process.
//  /// The process completes when the async computation completes.
//  let startChildAsync (a:Async<unit>) : Async<Proc> = async {
//    let! ct = Async.CancellationToken
//    let p = proc ()
//    let cts = CancellationTokenSource.CreateLinkedTokenSource (ct, p.cancellation)
//    Async.startThreadPoolWithContinuations (
//      a, 
//      (fun () -> IVar.tryPut () p.proc |> ignore), 
//      (fun e -> IVar.tryError e p.proc |> ignore), 
//      ignore, 
//      cts.Token)
//    return p }
//
//  let link (parent:Proc) (child:Proc) =
//    parent.proc.Task 
//    |> Task.extend (fun t ->
//      if t.IsCanceled then IVar.tryCancel child.proc
//      elif t.IsFaulted then IVar.tryError t.Exception child.proc
//      else IVar.tryPut t.Result child.proc)
//    |> ignore
//
//  let private chooseTaskAsTask (t:Task<'a>) (a:Async<'a>) = async {
//    let! a = Async.StartChildAsTask a
//    return Task.WhenAny (t, a) |> Task.join }
//
//  let private chooseTask (t:Task<'a>) (a:Async<'a>) : Async<'a> =
//    chooseTaskAsTask t a |> Async.bind Async.AwaitTask
//
//  /// Returns an async computation which runs the argument computation
//  /// but if the process fails, escalates the failure.
//  let tryAsync (p:Proc) (a:Async<'a>) : Async<'a> =
//    chooseTask (errorTask p) a

//
///// A stateful node.
//[<NoEquality;NoComparison;AutoSerializable(false);>]
//type Node<'a> = private {
//  
//  /// The node state.
//  state : MVar<NodeState<'a>>
//  
//  /// The process handle for the node.
//  proc : IVar<unit> }
//
///// An instance of node state.
//and [<NoEquality;NoComparison;AutoSerializable(false)>] NodeState<'a> = private {
//  
//  /// The state.
//  state : 'a
//
//  /// The epoch with respect to the parent node.
//  epoch : int
//
//  /// The process handle for the node state.
//  proc : IVar<unit> }
//
///// Operations on nodes.
//module Node =
//
//  open FSharp.Control
//
//  let private newNodeState (n:Node<'a>) (a:'a) (epoch:int) =
//    let ns = { NodeState.state = a ; epoch = epoch ; proc = IVar.create () }
//    IVar.linkTask ns.proc n.proc.Task |> ignore
//    ns
//
//  /// Creates an empty, unlinked node.
//  let empty<'a> : Node<'a> =
//    { state = MVar.create () ; proc = IVar.create () }
//
//  /// Creates a filled, unlinked node.
//  let full (a:'a) : Node<'a> =
//    let ns = { NodeState.state = a ; epoch = 0 ; proc = IVar.create () }
//    { state = MVar.createFull ns ; proc = IVar.create () }
//  
//  let link (parent:Node<'a>) (child:Node<'b>) =
//    IVar.linkTask child.proc parent.proc.Task |> ignore
//
//  let linkState (parent:NodeState<'a>) (child:NodeState<'b>) =
//    IVar.linkTask child.proc parent.proc.Task |> ignore
//
//  /// Kills the node permanently.
//  let kill (n:Node<'a>) =
//    IVar.tryCancel n.proc |> ignore
//
//  /// Returns the current node state.
//  let nodeState (n:Node<'a>) : Async<NodeState<'a>> =
//    MVar.get n.state
//
//  /// Returns a stream of consecutive node states starting with the current state.
//  let nodeStates (n:Node<'a>) : AsyncSeq<NodeState<'a>> =
//    failwith ""
//  
//  /// Returns the current state.
//  let state (n:Node<'a>) : Async<'a> =
//    nodeState n |> Async.map (fun s -> s.state)
//
//  /// Returns a stream of consecutive states starting with the current state.
//  let states (n:Node<'a>) : AsyncSeq<'a> =
//    nodeStates n |> AsyncSeq.map (fun s -> s.state)
//
//  /// Returns a task which completes when the node state process completes.
//  /// If completes successfully, returns None, otherwise returns the error.
//  let failure (ns:NodeState<'a>) : Task<exn option> =
//    ns.proc.Task
//    |> Task.extend (fun t ->
//      if t.IsFaulted then Some (t.Exception :> _)
//      else None)
//
//  /// Returns a stream of failures.
//  let failures (n:Node<'a>) : AsyncSeq<_> =
//    nodeStates n
//    |> AsyncSeq.chooseAsync (fun ns -> async {
//      let! procResult = failure ns |> Async.AwaitTask
//      match procResult with
//      | None -> 
//        return None
//      | Some e -> 
//        return Some (ns,e) })
//
//  let put (n:Node<'a>) (a:'a) : Async<NodeState<'a>> =
//    n.state
//    |> MVar.putOrUpdateAsync (fun ns -> async {
//      let epoch =
//        match ns with
//        | Some ns -> 
//          IVar.tryPut () ns.proc |> ignore
//          ns.epoch + 1
//        | None -> 0
//      return newNodeState n a epoch })
//
//  /// Updates the current node state using the specified function.
//  /// The node state is completed.
//  let update (n:Node<'a>) (f:NodeState<'a> -> Async<'a>) : Async<NodeState<'a>> =
//    n.state
//    |> MVar.updateAsync (fun ns -> async {
//      // TODO: OCC
//      let! a' = f ns
//      IVar.tryPut () ns.proc |> ignore
//      return newNodeState n a' (ns.epoch + 1) })
//
//  /// Creates a recovery process for the node.
//  let recovery (n:Node<'a>) (f:NodeState<'a> * exn -> Async<'a>) : Async<unit> =
//    failures n
//    |> AsyncSeq.iterAsync (fun (_,ex) -> async {
//      let! _ns = update n (fun ns -> f (ns,ex))
//      return () })
//
//  /// Puts the node state into a faulted state.
//  let fail (ns:NodeState<'a>) (e:exn) =
//    IVar.tryError e ns.proc |> ignore
//
//  /// Starts a computation as a child of a node.
//  /// If the node is closed, the computation is cancelled.
//  /// If the child computation fails, the node is failed.
//  let startNodeChildTask (n:Node<'a>) (a:Async<'b>) =
//    let cts = new CancellationTokenSource()
//    n.proc.Task |> Task.extend (fun _ -> cts.Cancel ()) |> ignore
//    let t = Async.StartAsTask (a, cancellationToken = cts.Token)
//    t 
//    |> Task.extend (fun t -> 
//      if t.IsFaulted then 
//        IVar.tryError t.Exception n.proc |> ignore
//      else ())
//    |> ignore
//    t
//        
//  let extend (f:NodeState<'a> -> 'b) (n:Node<'a>) : Node<'b> =
//    let n' = empty
//    link n n'
//    let loop =
//      nodeStates n
//      |> AsyncSeq.iterAsync (fun ns -> async {
//        let! ns' = put n' (f ns)
//        linkState ns ns'
//        return () })
//    startNodeChildTask n loop |> ignore
//    n'
//
//  let recoverable (n:Node<'a>) (op:'a -> 'i -> Async<'o>) : 'i -> Async<'o> =
//    let rec go i = async {
//      let! ns = nodeState n
//      let! o = op ns.state i
//      // TODO: check
//      if true then 
//        return o
//      else
//        fail ns (exn())
//        return! go i }
//    go
//
//
//module NoteTest =
//  
//  let test = async {
//
//    let node = Node.empty<string>
//    let! _ = Node.put node "cluster state"
//
//    // recovery process
//    let! _ = 
//      Async.StartChild <|
//        Node.recovery node (fun (ns,err) -> async {
//          printfn "recovering epoch=%i error=%O" ns.epoch err
//          return ns.state })
//      
//    
//
//    return () }

  