﻿[<AutoOpen>]
module internal Kafunk.Resource

open System
open System.Threading
open Kafunk

/// A result of a resource-dependent operation.
type ResourceResult<'a, 'e> = Result<'a, ResourceErrorAction<'a, 'e>>

/// The action to take when a resource-dependent operation fails.
and ResourceErrorAction<'a, 'e> =
  
  /// Recover the resource and return the specified result without retrying.
  | RecoverResume of 'e * 'a
    
  /// Recover the resource and retry the operation.
  | RecoverRetry of 'e

  /// Retry the operation without recovery.
  | Retry


/// A generation of a resource lifecycle.
type internal ResourceEpoch<'r> = {
  resource : 'r
  closed : CancellationTokenSource
  version : int
}

/// A recoverable resource, supporting resource-dependant operations.
type Resource<'r> internal (create:CancellationToken -> 'r option -> Async<'r>, handle:('r * int * obj * exn) -> Async<unit>) =
      
  let Log = Log.create "Resource"
  let cell : MVar<ResourceEpoch<'r>> = MVar.create ()
  let name = typeof<'r>.Name

  let create (prevEpoch:ResourceEpoch<'r> option) = async {
    let closed = new CancellationTokenSource()
    let version = 
      match prevEpoch with
      | Some prev ->
        prev.version + 1
      | None ->
        0
    let! res = create closed.Token (prevEpoch |> Option.map (fun e -> e.resource)) |> Async.Catch
    match res with
    | Success r ->
      return { resource = r ; closed = closed ; version = version }
    | Failure e ->
      return raise e }

  let recover (req:obj) (ex:exn) (callingEpoch:ResourceEpoch<'r>) = async {
    if callingEpoch.closed.IsCancellationRequested then
      return failwithf "resource_recovery_failed|version=%i" callingEpoch.version
    else
      callingEpoch.closed.Cancel ()
    do! handle (callingEpoch.resource, callingEpoch.version, req, ex)
    let! ep' = create (Some callingEpoch)
    return ep' }

  member internal __.Get () =
    MVar.get cell |> Async.map (fun ep -> ep.resource)

  member internal __.Create () = async {
    return! cell |> MVar.putOrUpdateAsync create }

  member internal __.TryGetVersion () =
    MVar.getFastUnsafe cell |> Option.map (fun e -> e.version)

  member private __.Recover (callingEpoch:ResourceEpoch<'r>, req:obj, ex:exn) =
    let update currentEpoch = async {
      if currentEpoch.version = callingEpoch.version then
        try
          let! ep2 = recover req ex callingEpoch
          return ep2
        with ex ->
          Log.trace "recovery_failed|type=%s error=%O" name ex
          return raise ex
      else
        Log.trace "resource_already_recovered|calling_version=%i current_version=%i" callingEpoch.version currentEpoch.version
        return currentEpoch }
    cell |> MVar.updateAsync update
    
  member internal __.Timeout<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b option> =
    fun a -> async {
      let! ep = MVar.get cell
      return! op ep.resource a |> Async.cancelWithToken ep.closed.Token }

  member internal __.InjectResult<'a, 'b> (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>), rp:RetryPolicy, a:'a) : Async<'b> =
    let rec go (rs:RetryState) = async {
      let! ep = MVar.get cell
      //let ep = MVar.getFastUnsafe cell |> Option.get
      let! b = op ep.resource a
      match b with
      | Success b -> 
        return b
      | Failure (RecoverResume (ex,b)) ->
        let! _ = __.Recover (ep, a, ex)
        return b
      | Failure (RecoverRetry ex) ->
        Log.trace "recovering_and_retrying_after_failure|name=%s attempt=%i error=%O" name rs.attempt ex
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          Log.trace "escalating_after_retry_attempts_depleted|name=%s attempt=%i error=%O" name rs.attempt ex
          return raise ex
        | Some rs' ->
          let! _ = __.Recover (ep, a, ex)
          return! go rs'
      | Failure (Retry) ->
        Log.trace "retrying|name=%s attempt=%i" name rs.attempt
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          Log.trace "escalating_after_retry_attempts_depleted|name=%s attempt=%i" name rs.attempt
          return raise (exn("Escalating after retry."))
        | Some rs' ->
          return! go rs' }
    go RetryState.init
        
  member internal __.InjectResult<'a, 'b> (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = MVar.get cell
      //let ep = MVar.getFastUnsafe cell |> Option.get
      let! res = op ep.resource a
      match res with
      | Success b -> 
        return b
      | Failure (RecoverResume (ex,b)) ->
        let! _ = __.Recover (ep, a, ex)
        return b
      | Failure (RecoverRetry ex) ->
        let! _ = __.Recover (ep, a, ex)
        return! go a
      | Failure (Retry) ->
        return! go a }
    return go }

  member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = MVar.get cell
      //let ep = MVar.getFastUnsafe cell |> Option.get
      try
        return! op ep.resource a
      with ex ->
        let! _ = __.Recover (ep, box a, ex)
        return! go a }
    return go }

  interface IDisposable with
    member __.Dispose () = ()

/// Operations on resources.
module Resource =
    
  let recoverableRecreate (create:CancellationToken -> 'r option -> Async<'r>) (handleError:('r * int * obj * exn) -> Async<unit>) = async {
    let r = new Resource<_>(create, handleError)
    let! _ = r.Create()
    return r }
  
  let get (r:Resource<'r>) : Async<'r> =
    r.Get ()

  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op

  let injectResult (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.InjectResult op

  let injectWithRecovery (r:Resource<'r>) (rp:RetryPolicy) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) (a:'a) : Async<'b> =
    r.InjectResult (op, rp, a)

  let timeoutIndep (r:Resource<'r>) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    r.Timeout (fun _ -> f)