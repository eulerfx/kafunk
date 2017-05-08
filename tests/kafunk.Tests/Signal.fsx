#load "Refs.fsx"

open Kafunk
open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks


type private MVarReq<'a> =
  | PutAsync of Async<'a> * IVar<'a>
  | UpdateAsync of update:('a -> Async<'a>)
  | PutOrUpdateAsync of update:('a option -> Async<'a>) * IVar<'a>
  | Get of IVar<'a>
  | Take of IVar<'a>

/// A serialized variable.
type MVar<'a> internal (?a:'a) =

  let [<VolatileField>] mutable state : 'a option = None

  let mbp = MailboxProcessor.Start (fun mbp -> async {
    let rec init () = async {
      return! mbp.Scan (function
        | PutAsync (a,rep) ->
          Some (async {
            try
              let! a = a
              state <- Some a
              IVar.put a rep
              return! loop a
            with ex ->
              state <- None
              IVar.error ex rep
              return! init () })
        | PutOrUpdateAsync (update,rep) ->
          Some (async {
            try
              let! a = update None
              state <- Some a
              IVar.put a rep
              return! loop (a)
            with ex ->
              state <- None
              IVar.error ex rep
              return! init () })
        | _ ->
          None) }
    and loop (a:'a) = async {
      let! msg = mbp.Receive()
      match msg with
      | PutAsync (a',rep) ->
        try
          let! a = a'
          state <- Some a
          IVar.put a rep
          return! loop (a)
        with ex ->
          state <- Some a
          IVar.error ex rep
          return! loop (a)
      | PutOrUpdateAsync (update,rep) ->
        try
          let! a = update (Some a)
          state <- Some a
          IVar.put a rep
          return! loop (a)
        with ex ->
          state <- Some a
          IVar.error ex rep
          return! loop (a)
      | Get rep ->
        IVar.put a rep
        return! loop (a)
      | Take (rep) ->
        state <- None
        IVar.put a rep
        return! init ()
      | UpdateAsync f ->
        let! a = f a
        return! loop a }
    match a with
    | Some a ->
      state <- Some a
      return! loop (a)
    | None -> 
      return! init () })

  do mbp.Error.Add (fun x -> printfn "|MVar|ERROR|%O" x) // shouldn't happen
  
  let postAndAsyncReply f = async {
    let ivar = IVar.create ()
    mbp.Post (f ivar)
    return! IVar.get ivar }

  member __.Get () : Async<'a> =
    postAndAsyncReply (Get)

  member __.Take () : Async<'a> =
    postAndAsyncReply (fun tcs -> Take(tcs))

  member __.GetFast () : 'a option =
    state

  member __.Put (a:'a) : Async<'a> =
    __.PutAsync (async.Return a)

  member __.PutAsync (a:Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutAsync (a,ch))

  member __.UpdateStateAsync (update:'a -> Async<'a * 's>) : Async<'s> = async {
    let rep = IVar.create ()
    let up a = async {
      try
        let! (a,s) = update a
        state <- Some a
        IVar.put s rep
        return a
      with ex ->
        state <- Some a
        IVar.error ex rep
        return a  }
    mbp.Post (UpdateAsync up)
    return! IVar.get rep }

  member __.PutOrUpdateAsync (update:'a option -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutOrUpdateAsync (update,ch))

  member __.Update (f:'a -> 'a) : Async<'a> =
    __.UpdateAsync (f >> async.Return)

  member __.UpdateAsync (update:'a -> Async<'a>) : Async<'a> =
    __.UpdateStateAsync (update >> Async.map diag)

  interface IDisposable with
    member __.Dispose () = (mbp :> IDisposable).Dispose()

/// Operations on serialized variables.
module MVar =
  
  /// Creates an empty MVar.
  let create () : MVar<'a> =
    new MVar<_>()

  /// Creates a full MVar.
  let createFull (a:'a) : MVar<'a> =
    new MVar<_>(a)

  /// Gets the value of the MVar.
  let get (c:MVar<'a>) : Async<'a> =
    async.Delay (c.Get)

  /// Takes an item from the MVar.
  let take (c:MVar<'a>) : Async<'a> =
    async.Delay (c.Take)
  
  /// Returns the last known value, if any, without serialization.
  /// NB: unsafe because the value may be null, but helpful for supporting overlapping
  /// operations.
  let getFastUnsafe (c:MVar<'a>) : 'a option =
    c.GetFast ()

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let put (a:'a) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.Put a)

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let putAsync (a:Async<'a>) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.PutAsync a)

  /// Puts a new value into an MVar or updates an existing value.
  /// Returns the value that was put or the updated value.
  let putOrUpdateAsync (update:'a option -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.PutOrUpdateAsync update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let updateStateAsync (update:'a -> Async<'a * 's>) (c:MVar<'a>) : Async<'s> =
    async.Delay (fun () -> c.UpdateStateAsync update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let update (update:'a -> 'a) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.Update update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let updateAsync (update:'a -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.UpdateAsync update)



/// ------------------------------------------------------------------------------------------------



type Source<'a> = {
  value : MVar<'a>
  tail : AsyncSeqSrc<'a> }

type Signal<'a> = {
  value : Async<'a>
  stream : AsyncSeq<'a> }

module Source =
  
  [<GeneralizableValue>]
  let empty<'a> : Source<'a> = 
    { value = MVar.create () ; tail = AsyncSeqSrc.create () }

  let put (src:Source<'a>) (a:'a) = async {
    let! _ = MVar.put a src.value
    AsyncSeqSrc.put a src.tail }

  let putAsync (src:Source<'a>) (a:Async<'a>) = async {
    let! a = a
    let! _ = MVar.put a src.value
    AsyncSeqSrc.put a src.tail }
  
  let updateAsync (src:Source<'a>) (f:'a -> Async<'a * 's>) : Async<'s> = async {
    let! (s,a) = 
      src.value
      |> MVar.updateStateAsync (fun a -> async {
          let! (a,s) = f a
          return (a,(s,a)) })
    AsyncSeqSrc.put a src.tail
    return s }

  let stream (src:Source<'a>) : AsyncSeq<'a> = asyncSeq {
    let! a = MVar.get src.value
    yield a
    yield! src.tail |> AsyncSeqSrc.toAsyncSeq }

  let tap (src:Source<'a>) : Signal<'a> =
    { Signal.value = MVar.get src.value ; stream = AsyncSeqSrc.toAsyncSeq src.tail }
  

module Signal =
  
  let constant (a:'a) : Signal<'a> =
    { value = async.Return a ; stream = AsyncSeq.singleton a }

  let value (s:Signal<'a>) : Async<'a> =
    s.value

  let stream (s:Signal<'a>) : AsyncSeq<'a> =
    s.stream

  let map (f:'a -> 'b) (s:Signal<'a>) : Signal<'b> =
    { value = Async.map f s.value ; stream = AsyncSeq.map f s.stream }

  let mapAsync (f:'a -> Async<'b>) (s:Signal<'a>) : Signal<'b> =
    { value = Async.bind f s.value ; stream = AsyncSeq.mapAsync f s.stream }

  let combineLatestWith (f:'a -> 'b -> 'c) (a:Signal<'a>) (b:Signal<'b>) : Signal<'c> =
    let src = Source.empty
    (a.stream, b.stream) 
    ||> AsyncSeq.combineLatestWithAsync (fun a b -> Source.put src (f a b))
    |> AsyncSeq.iter ignore
    |> Async.Start
    Source.tap src
  
  let ap (f:Signal<'a -> 'b>) (a:Signal<'a>) : Signal<'b> =
    combineLatestWith (fun f a -> f a) f a

  let toFun (s:Signal<'a -> 'b>) : Async<'a -> 'b> = async {
    let! f = s.value
    let f = ref f
    stream s
    |> AsyncSeq.skip 1
    |> AsyncSeq.iter (fun f' -> Interlocked.Exchange (f, f') |> ignore)
    |> Async.Start
    return fun a -> !f a }

        
