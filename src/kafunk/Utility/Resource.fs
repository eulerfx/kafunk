[<AutoOpen>]
module internal Kafunk.Resource

open System
open System.Threading
open Kafunk
open FSharp.Control

/// A result of a resource-dependent operation.
type ResourceResult<'a, 'e> = Result<'a, ResourceErrorAction<'a, 'e>>

/// The action to take when a resource-dependent operation fails.
and ResourceErrorAction<'a, 'e> =
  
  /// Recover the resource and return the specified result without retrying.
  | RecoverResume of 'e * 'a
    
  /// Recover the resource and retry the operation.
  | RecoverRetry of 'e

  /// Retry without recovery.
  | Retry of 'e


/// A generation of a resource lifecycle.
type internal ResourceEpoch<'r> = {
  resource : 'r
  closed : CancellationTokenSource
  version : int }

type IResource<'r> =
  abstract Get : unit -> Async<ResourceEpoch<'r>>
  abstract InjectResult<'a, 'b> : ('r -> 'a -> Async<ResourceResult<'b, exn>>) * RetryPolicy * 'a -> Async<'b>

/// A recoverable resource, supporting resource-dependant operations.
type Resource<'r> internal 
  (create:CancellationToken -> 'r option -> Async<'r>, 
   handleError:('r * int * obj * exn) -> Async<unit>) =
      
  let Log = Log.create "Kafunk.Resource"
  let cell : SVar<ResourceEpoch<'r> option> = SVar.createFull None
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
      return Some { resource = r ; closed = closed ; version = version }
    | Failure e ->
      Log.warn "resource_creation_failed|type=%s error=\"%O\"" name e
      return raise e }

  /// Gets the current instance of the resource, creating it if needed.
  member internal __.Get () = async {
    let! r = SVar.get cell
    match r with
    | Some r -> 
      return r
    | None -> 
      let! _ = __.Create ()
      return! __.Get () }

  member internal __.Create () = async {
    return! cell |> SVar.updateAsync create }

  member private __.Close (callingEpoch:ResourceEpoch<'r>, req:obj, ex:exn) = async {
    let! _ =
      cell 
      |> SVar.updateAsync (fun currentEpoch -> async {
          match currentEpoch with
          | Some currentEpoch -> 
            if currentEpoch.version = callingEpoch.version then
              try
                // TODO: why?
                if callingEpoch.closed.IsCancellationRequested then
                  return failwithf "recovery_failed|type=%s version=%i" name callingEpoch.version
                else
                  callingEpoch.closed.Cancel ()
                do! handleError (callingEpoch.resource, callingEpoch.version, req, ex)
                return None
              with ex ->
                let errMsg = sprintf "resource_close_failed|type=%s version=%i error=\"%O\"" name currentEpoch.version ex
                Log.error "%s" errMsg
                return raise (exn(errMsg, ex))
            else
              Log.trace "resource_already_closed|type=%s calling_version=%i current_version=%i" 
                name callingEpoch.version currentEpoch.version
              return Some currentEpoch
          | None ->
            Log.trace "resource_already_closed|type=%s calling_version=%i" name callingEpoch.version
            return None })
    return () }
    
  member internal __.Timeout<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b option> =
    fun a -> async {
      let! ep = __.Get ()
      return! op ep.resource a |> Async.cancelWithToken ep.closed.Token }

  member internal __.InjectResult<'a, 'b> (op:'r -> 'a -> Async<ResourceResult<'b, exn>>, rp:RetryPolicy, a:'a) : Async<'b> =
    let rec go (rs:RetryState) = async {
      let! ep = __.Get ()
      let! b = op ep.resource a
      match b with
      | Success b -> 
        return b
      | Failure (Retry e) ->
        Log.trace "retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          return! go rs'
      | Failure (RecoverResume (ex,b)) ->
        do! __.Close (ep, a, ex)
        return b
      | Failure (RecoverRetry e) ->
        Log.trace "recovering_and_retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          do! __.Close (ep, a, e)
          return! go rs' }
    go RetryState.init

  member internal __.InjectResult2<'a, 'b> (ep:ResourceEpoch<'r>, op:'r -> 'a -> Async<ResourceResult<'b, exn>>, rp:RetryPolicy, a:'a) : Async<'b> =
    let rec go (ep:ResourceEpoch<'r>) (rs:RetryState) = async {
      let! b = op ep.resource a
      match b with
      | Success b -> 
        return b
      | Failure (Retry e) ->
        Log.trace "retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          return! go ep rs'
      | Failure (RecoverResume (ex,b)) ->
        do! __.Close (ep, a, ex)
        return b
      | Failure (RecoverRetry e) ->
        Log.trace "recovering_and_retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          do! __.Close (ep, a, e)
          return! go ep rs' }
    go ep RetryState.init
        
  member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = __.Get ()
      try
        return! op ep.resource a
      with ex ->
        do! __.Close (ep, box a, ex)
        return! go a }
    return go }

  member internal __.Map<'b> (f:'r -> Async<'b>) : IResource<'b> = 
    let c : MVar<(ResourceEpoch<'r> * ResourceEpoch<'b>) option> = MVar.createFull None
    cell
    |> SVar.tap
    |> AsyncSeq.iterAsync (fun r -> async {
        match r with
        | Some r ->
          let! b = f r.resource
          let br = { ResourceEpoch.resource = b ; closed = r.closed ; version = r.version }
          let! _ = MVar.put (Some (r,br)) c
          return ()
        | None ->
          let! _ = MVar.put None c
          return () })
    |> Async.Start
    { new IResource<'b> with        
        member this.Get () = async {
          let! x = MVar.get c
          match x with
          | Some (_,ep) -> return ep
          | None -> 
            let! _ = __.Create ()
            return! this.Get () }
        member this.InjectResult (op,rp,a) =
          __.InjectResult ((fun _ a -> op (failwith "") a),rp,a) }

  interface IDisposable with
    member __.Dispose () = ()

/// Operations on resources.
module Resource =
    
  let recoverableRecreate (create:CancellationToken -> 'r option -> Async<'r>) (handleError:('r * int * obj * exn) -> Async<unit>) = async {
    let r = new Resource<_>(create, handleError)
    return r }
  
  let get (r:Resource<'r>) : Async<'r> = async {
    let! ep = r.Get ()
    return ep.resource }

  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op

  let injectWithRecovery (rp:RetryPolicy) (r:Resource<'r>) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) (a:'a) : Async<'b> =
    r.InjectResult (op, rp, a)

  let timeoutIndep (r:Resource<'r>) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    r.Timeout (fun _ -> f)

  let mapAsync (f:'a -> Async<'b>) (r:Resource<'a>) : IResource<'b> = 
    r.Map f