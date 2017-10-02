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

  /// Close the resource and escalate the error.
  | Escalate of 'e

/// A generation of a resource lifecycle.
type ResourceEpoch<'r> = {
  resource : 'r
  token : EpochToken }

and EpochToken = {
  version : int 
  closed : CancellationToken }
  
type IResource<'r> =
  abstract Get : unit -> Async<ResourceEpoch<'r>>
  abstract InjectResult<'a, 'b> : ('r -> 'a -> Async<ResourceResult<'b, exn>>) * RetryPolicy * 'a -> Async<'b>

/// A recoverable resource, supporting resource-dependant operations.
type Resource<'r> internal 
  (create:int -> CancellationToken -> 'r option -> Async<'r>, 
   handleError:('r * int * obj * exn) -> Async<unit>) =
      
  let Log = Log.create "Kafunk.Resource"
  let cell : SVar<ResourceEpoch<'r> option> = SVar.createFull None
  let name = typeof<'r>.Name

  let create (prevEpoch:ResourceEpoch<'r> option) = async {
    let closed = new CancellationTokenSource()
    let version = 
      match prevEpoch with
      | Some prev ->
        prev.token.version + 1
      | None ->
        0
    let token = { version = version ; closed = closed.Token }
    let! res = create version closed.Token (prevEpoch |> Option.map (fun e -> e.resource)) |> Async.Catch
    match res with
    | Success r ->
      return Some { resource = r ; token = token }
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

  member internal __.Close (callingEpoch:EpochToken, req:obj, ex:exn) = async {
    let! _ =
      cell 
      |> SVar.updateAsync (fun currentEpoch -> async {
          match currentEpoch with
          | Some currentEpoch -> 
            if currentEpoch.token.version = callingEpoch.version then
              try
                // TODO: why?
                if callingEpoch.closed.IsCancellationRequested then
                  return failwithf "resource_close_failed|type=%s version=%i" name callingEpoch.version
                else
                  //currentEpoch.closed.Cancel ()
                  ()
                do! handleError (currentEpoch.resource, callingEpoch.version, req, ex)
                return None
              with ex ->
                let errMsg = sprintf "resource_close_failed|type=%s version=%i error=\"%O\"" name callingEpoch.version ex
                Log.error "%s" errMsg
                return raise (exn(errMsg, ex))
            else
              Log.trace "resource_already_closed|type=%s calling_version=%i current_version=%i" 
                name callingEpoch.version callingEpoch.version
              return Some currentEpoch
          | None ->
            Log.trace "resource_already_closed|type=%s calling_version=%i" name callingEpoch.version
            return None })
    return () }
    
  member internal __.Timeout<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b option> =
    fun a -> async {
      let! ep = __.Get ()
      return! op ep.resource a |> Async.cancelWithToken ep.token.closed }

  member internal __.InjectResult<'a, 'b> (op:'r -> 'a -> Async<ResourceResult<'b, exn>>, rp:RetryPolicy, a:'a) : Async<'b> =
    let rec go (rs:RetryState) = async {
      let! ep = __.Get ()
      let! b = op ep.resource a
      match b with
      | Success b -> 
        return b
      | Failure (Retry e) ->
        Log.trace "retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.token.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.token.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          return! go rs'
      | Failure (RecoverResume (ex,b)) ->
        do! __.Close (ep.token, a, ex)
        return b
      | Failure (RecoverRetry e) ->
        Log.trace "recovering_and_retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.token.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.token.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          do! __.Close (ep.token, a, e)
          return! go rs'
      | Failure (Escalate e) ->
        return raise (exn("")) }
    go RetryState.init
        
  member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = __.Get ()
      try
        return! op ep.resource a
      with ex ->
        do! __.Close (ep.token, box a, ex)
        return! go a }
    return go }

  //member internal __.Map<'b> (f:'r -> Async<'b>) : IResource<'b> = 
  //  let c : MVar<(ResourceEpoch<'r> * ResourceEpoch<'b>) option> = MVar.createFull None
  //  cell
  //  |> SVar.tap
  //  |> AsyncSeq.iterAsync (fun r -> async {
  //      match r with
  //      | Some r ->
  //        let! b = f r.resource
  //        let br = { ResourceEpoch.resource = b ; closed = r.closed ; version = r.version }
  //        let! _ = MVar.put (Some (r,br)) c
  //        return ()
  //      | None ->
  //        let! _ = MVar.put None c
  //        return () })
  //  |> Async.Start
  //  { new IResource<'b> with        
  //      member this.Get () = async {
  //        let! x = MVar.get c
  //        match x with
  //        | Some (_,ep) -> return ep
  //        | None -> 
  //          let! _ = __.Create ()
  //          return! this.Get () }
  //      member this.InjectResult (op,rp,a) =
  //        __.InjectResult ((fun _ a -> op (failwith "") a),rp,a) }

  interface IDisposable with
    member __.Dispose () = ()

/// Operations on resources.
module Resource =
    
  let recoverableRecreate (create:int -> CancellationToken -> 'r option -> Async<'r>) (handleError:('r * int * obj * exn) -> Async<unit>) = async {
    let r = new Resource<_>(create, handleError)
    return r }
  
  let get (r:Resource<'r>) : Async<'r> = async {
    let! ep = r.Get ()
    return ep.resource }

  let close (r:Resource<'r>) (epoch:EpochToken) =
    r.Close (epoch, null, null)

  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op

  let injectWithRecovery (rp:RetryPolicy) (r:Resource<'r>) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) (a:'a) : Async<'b> =
    r.InjectResult (op, rp, a)

  let timeoutIndep (r:Resource<'r>) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    r.Timeout (fun _ -> f)

  //let mapAsync (f:'a -> Async<'b>) (r:Resource<'a>) : IResource<'b> = 
  //  r.Map f




type HandleEpoch = {
  version : int 
  state : IVar<unit> }

type Handle<'a> (create:HandleEpoch -> 'a option -> Async<'a>, close:HandleEpoch -> 'a -> (obj * exn) option -> Async<unit>) = 
  
  let epochs : SVar<(HandleEpoch * 'a) option> = SVar.createFull None
  let node = IVar.create ()
  // TODO: link handle and instance state

  let createEpoch (current:(HandleEpoch * 'a) option) = async {
    let prevA = current |> Option.map snd
    let epochState = IVar.create ()
    let ep' =
      match current with
      | Some (ep,_) -> 
        IVar.tryPut ep.state |> ignore
        { version = ep.version + 1 ; state = epochState }
      | None -> 
        { version = 0 ; state = epochState }    
    let! a = create ep' prevA
    return Some (ep',a) }

  let closeEpoch (ep:HandleEpoch) (current:(HandleEpoch * 'a) option) = async {
    match current with
    | None ->      
      return None
    | Some (ep',a) ->
      if ep'.version = ep.version then
        do! close ep a None
        return None
      else
        return Some (ep',a) }

  member __.Get () = async {
    let! epoch = epochs |> SVar.get
    match epoch with
    | Some e -> return e
    | None -> 
      let! _ = __.Open ()
      return! __.Get () }
 
  member private __.Open () = 
    epochs |> SVar.updateAsync createEpoch

  member __.Close (ep:HandleEpoch) = 
    epochs |> SVar.updateAsync (closeEpoch ep) |> Async.Ignore

  interface IDisposable with
    member __.Dispose () = ()

module Handle =
  
  let create (create:HandleEpoch -> 'a option -> Async<'a>) (close:HandleEpoch -> 'a -> (obj * exn) option -> Async<unit>) =
    new Handle<'a>(create, close)

  let get (h:Handle<'a>) : Async<HandleEpoch * 'a> = 
    h.Get ()

  let close (h:Handle<'a>) (ep:HandleEpoch) : Async<unit> = 
    h.Close ep

  


type HandleOpAction<'a, 'e> =
  | Retry of 'e
  | Resume of 'a
  | Escalate of 'e

module HandleOp =

  // TODO: timeout?
  // TODO: recovery action dependant on the error (ie refresh group coordinator when that is stale)
  let applyAsyncRetry (h:Handle<'a>) (rp:RetryPolicy) (f:HandleEpoch -> 'a -> Async<Result<'b, HandleOpAction<'b, 'e>>>) : Async<Result<'b, 'e>> = async {
    let! (e,a) = Handle.get h
    let rec go (rs:RetryState) (ep:HandleEpoch) (a:'a) = async {
      let! res = f ep a
      match res with
      | Success b -> 
        return Success b
      | Failure (Retry err) -> 
        do! Handle.close h ep
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | Some rs' ->        
          let! (e,a) = Handle.get h
          return! go rs' e a
        | None ->
          return Failure err
      | Failure (Resume b) ->
        do! Handle.close h ep
        return Success b
      | Failure (Escalate err) ->
        do! Handle.close h ep
        return Failure err }
    return! go RetryState.init e a }