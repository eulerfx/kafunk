#load "Refs.fsx"
#r "bin/release/hopac.core.dll"
#r "bin/release/hopac.platform.dll"
#r "bin/release/hopac.dll"
#time "on"

open Hopac
open Hopac.Infixes
open System

module Stream =
  
  let bufferByCountAndTime (count:int) (time:TimeSpan) (s:Stream<'a>) =
    let rec go s (buf:ResizeArray<_>) =
      let timeOrNext = 
        (Hopac.timeOut time |> Alt.afterFun Choice2Of2)
        <|> 
        (Promise.read s |> Alt.afterFun Choice1Of2)
      timeOrNext
      |> Alt.afterJob (function
        | Choice1Of2 Stream.Nil -> 
          if buf.Count > 0 then Job.result (Stream.cons (buf.ToArray()) Stream.nil)
          else Job.result Stream.nil
        | Choice1Of2 (Stream.Cons (a,tl)) -> 
          buf.Add a
          if buf.Count = count then failwith ""
          else failwith ""
        | Choice2Of2 _ -> failwith "")
        
    go s (ResizeArray<_>())

    