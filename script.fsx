#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"

open System                                                                                         // Default .Net core library -  pow, rand, printfn functions
open Akka.Actor                                                                                     // Akka actor library
open Akka.Configuration                                                                             // Akka Configuration
open Akka.FSharp                                                                                    // Akka.FSharp library    
open Akka.TestKit                                                                                   // Akka Test Kit


let configuration =                                                                                 // Setting configuration to suppress dead letters
    ConfigurationFactory.ParseString(                                                               // Num dead letters to zero indicates no dead letter warnings are displayed after exit
        @"akka {
            log-dead-letters-during-shutdown : off
            log-dead-letters = 0                                                                    
        }")

let system = ActorSystem.Create("system", configuration)                                            // Creating Akka system with the configuration settings

let args = Environment.GetCommandLineArgs()                                                         // Fetching Environment variables

let rand = Random(777)                                                                              // Setting random state to 777

let timer = new System.Diagnostics.Stopwatch()                                                      // Setting the timer

let mutable Neighbours = Map.empty<int,List<int>>                                                   // Declaring Neighbor map to identify neighbor's list or each index

let mutable MsgCount = Map.empty<int,int>                                                           // Declaring MsgCount map to identify Messages received from other neighbor actors

let mutable NodeRatios = Map.empty<int,List<double>>                                                // Specific for pushsum - to store S,W values for each of the nodes

let mutable NodesConverged = []                                                                     // A list that identifies nodes that converged during the process

let mutable GlobalCount = 0                                                                         // A global count to understand how many nodes converged

type Message=                                                                                       // Setting a type message which contains all the message types 
    |Gossip of string                                                                               // Propagates Gossip 
    |Pushsum of double * double                                                                     // Propagates pushsum
    |Insert of int * string * string                                                                //
    |Done of string                                                                                 // OTHER HELPER
    |SecInsert of string * int                                                                      //   OBJECTS
    |GossipDone of string                                                                           //
    |PushSumDone of double * double                                                                 //
    
let Child (idx:int,len:int) (master: IActorRef) (mailbox :Actor<_>)=                                // Child Actor which listens to master
    let rec loop () = actor {
        let! message=mailbox.Receive ()                                                             // Receives message from mailbox
        match message with
        |Gossip(msg) ->                                                                             // Gossip Algorithm starts
            if MsgCount.Item(idx)<10 then                                                           // If Message count less than 10 indicates, it can still Gossip message to Neighbours
                MsgCount<-MsgCount.Add(idx,MsgCount.Item(idx)+1)
                if MsgCount.Item(idx)=10 && not(List.contains idx NodesConverged) then              // Checking if its a final message. If so -> call the it Done.
                    NodesConverged<-NodesConverged @ [idx]
                    master <! Done("done")                                                          // Inform parent node about it
                else
                    let RandomNeighbour=Neighbours.Item(idx).Item(rand.Next(0,len))                 // Get a RandomNeighbour through rand object
                    select ("/user/"+ string RandomNeighbour) mailbox.Context.System <! Gossip(msg) // Gossip to Random neighbour
                    select ("/user/"+ string idx) mailbox.Context.System <! GossipDone(msg)      // Keep Gossiping
            else 
                mailbox.Sender () <! GossipDone("done")                                             // Otherwise call it done and inform parent
        |GossipDone(msg) ->
            let RandomNeighbour=Neighbours.Item(idx).Item(rand.Next(0,len))                         // Take RandomNeighbour
            select ("/user/"+ string RandomNeighbour) mailbox.Context.System <! Gossip(msg)         // Spread the message
        |Pushsum(su,wt) ->                                                                          // PushSum Algorithm Starts
            if MsgCount.Item(idx)<3 then
                let mutable oldS=NodeRatios.Item(idx).Item(0)                                       // Take Old S
                let mutable oldW=NodeRatios.Item(idx).Item(1)                                       // Take Old W
                let oldRatio=oldS/oldW                                                              // Find oldRatio
                NodeRatios<-NodeRatios.Add(idx,[oldS+su;oldW+wt])                                   // Updating the ratios
                let mutable newS=NodeRatios.Item(idx).Item(0)                                       // Fetching new S
                let mutable newW=NodeRatios.Item(idx).Item(1)                                       // Fetching new W
                let newRatio=newS/newW                                                              // Finding new ratio
                let Difference=Math.Abs(newRatio-oldRatio)                                          // Finding difference between old and new ratios
                if(Difference<Math.Pow(10.0,-10.0)) then                                            // If it is less than 10e-10 then increment the count
                    MsgCount<-MsgCount.Add(idx,MsgCount.Item(idx)+1)
                if(MsgCount.Item(idx)=3) then                                                       // If msgcount remains same for 3 rounds, then call it done
                    master <! SecInsert("done",idx)
                newS<-newS/2.0                                                                      // Send half of the S to the random Neighbour
                newW<-newW/2.0                                                                      // Send half of the W to the random Neighbour
                NodeRatios<-NodeRatios.Add(idx,[newS;newW])                                         // Update New Node Ratios
                let RandomNeighbour=Neighbours.Item(idx).Item(rand.Next(0,len))
                select ("/user/"+ string RandomNeighbour) mailbox.Context.System <! Pushsum(newS,newW) // Propagate pushsum to random neighbor
                select ("/user/"+ string idx) mailbox.Context.System <! Pushsum(newS,newW)             // Continue propagating more 
            else
             mailbox.Sender () <! PushSumDone(su,wt)                                                // Else inform the parent to call it done
        |PushSumDone(sum,weight) ->                                                               
            let RandomNeighbour=Neighbours.Item(idx).Item(rand.Next(0,len))                         // Take random Neighbour
            select ("/user/"+ string RandomNeighbour) mailbox.Context.System <! Pushsum(sum,weight) // Propagate pushsum again to other nodes
        | _ -> ()                                                                                   // Unknown message leads to Nothing
        return! loop ()
    }
    loop ()

let nodes = int args.[3]                                                                            // Fetch nodes from command line
let topology = string args.[4]                                                                      // Fetch topology from command line
let gossipOrpushSum = string args.[5]                                                               // Fetch Algorithm from command line

let Parent (mailbox :Actor<_>) message =                                                            // Parent actor
    match message with
    |Insert(nodes,topology,algo) ->                                                                 // Understanding the topology
        match topology with
        |"line" ->                                                                                  // Line topology
            for i in 0..nodes-1 do                                                                  
                MsgCount<- MsgCount.Add(i,0)
                NodeRatios<-NodeRatios.Add(i,[i |> double;1.0])
                if i=0 then
                    Neighbours<-Neighbours.Add(i,[i+1])
                elif i=nodes-1 then
                    Neighbours<-Neighbours.Add(i,[i-1])
                else
                    Neighbours<-Neighbours.Add(i,[i-1;i+1])
        |"full"->                                                                                   // Full topology
            for i in 0..nodes-1 do
                MsgCount<- MsgCount.Add(i,0)
                NodeRatios<-NodeRatios.Add(i,[i |> double;1.0]) 
                let mutable tempList=[]
                for j in 0..nodes-1 do
                    if (i <> j) then
                        tempList<-tempList @ [j]
                Neighbours<-Neighbours.Add(i,tempList)
        |"2d"->                                                                                     // 2D topology
            let cSqrt=Math.Ceiling(Math.Sqrt(nodes |> float)) |> int
            for i in 0..nodes-1 do
                MsgCount<- MsgCount.Add(i,0)
                NodeRatios<-NodeRatios.Add(i,[i |> double;1.0])
                let mutable tempList=[]
                if ((i-cSqrt) >= 0) then
                    tempList<-tempList @ [i-cSqrt]

                if ((cSqrt+i) <= nodes-1) then
                    tempList<-tempList @ [cSqrt+i]

                if ((i % cSqrt) <> 0) then
                    tempList<-tempList @ [i-1]

                if (((i+1) % cSqrt) <> 0 || (i+1)<nodes) then
                    tempList<-tempList @ [i+1]

                Neighbours<-Neighbours.Add(i,tempList)
        |"imp2d"->                                                                                  // Imperfect 2D
            let mutable randnode=rand.Next(0,nodes)
            let cSqrt=Math.Ceiling(Math.Sqrt(nodes |> float)) |> int
            for i in 0..nodes-1 do
                MsgCount<- MsgCount.Add(i,0)
                NodeRatios<-NodeRatios.Add(i,[i |> double;1.0])
                let mutable tempList=[]
                if ((i-cSqrt) >= 0) then
                    tempList<-tempList @ [i-cSqrt]

                if ((cSqrt+i) <= nodes-1) then
                    tempList<-tempList @ [cSqrt+i]

                if ((i % cSqrt) <> 0) then
                    tempList<-tempList @ [i-1]

                if (((i+1) % cSqrt) <> 0 || (i+1)<nodes) then
                    tempList<-tempList @ [i+1]

                while (List.contains randnode tempList) do
                    randnode<-rand.Next(0,nodes) 
                tempList<-tempList @ [randnode]
                Neighbours<-Neighbours.Add(i,tempList)
        | _ -> ()
        let childRefs = [for i in 0..nodes-1 do yield (spawn system (string i) (Child(i,List.length (Neighbours.Item(i))) mailbox.Self))] // Creating Child References
        timer.Start()                                                                               // Setting timer to start to count milliseconds for Algorithm to finish
        if (algo ="gossip") then
            childRefs.Item(((0.05 * float nodes) |> int)) <! Gossip("hello")                        // Running the Gossip algorithm from the 5th percentage nodes
        elif (algo="pushsum") then
            childRefs.Item(((0.05 * float nodes) |> int)) <! Pushsum(0.0,0.0)                       // Running the Gossip algorithm from the 5th percentage nodes
    |Done(s)->                                                                                      
        GlobalCount<-GlobalCount+1                                                                  // Incrementing GobalCount when Done is called
        if GlobalCount=nodes then
            printfn "Num Nodes Converged: %i" GlobalCount
            printfn "Elapsed Time: %i" timer.ElapsedMilliseconds
            mailbox.Context.System.Terminate () |> ignore                                           // Terminating the actor system if globalcount reaches Num Nodes
    |SecInsert(pushDone,nodeId) ->
        GlobalCount<-GlobalCount+1
        if (GlobalCount = nodes) then
            printfn "Num Nodes Converged: %i" GlobalCount
            printfn "Elapsed Time: %i" timer.ElapsedMilliseconds
            mailbox.Context.System.Terminate () |> ignore                                           // Terminating the actor system if globalcount reaches Num Nodes
    | _ -> ()


let ParentRef=spawn system "Master" (actorOf2 Parent)                                               // Creating a high level actor 

printfn "nodes %i" nodes
printfn "topology %s" topology
printfn "gossip or pushsum %s" gossipOrpushSum

ParentRef <! Insert(nodes,topology,gossipOrpushSum)                                                 // Sending the message to parent which in turn sends the message to child actors
system.WhenTerminated.Wait ()                                                                       // Wait for the Actor system to terminate