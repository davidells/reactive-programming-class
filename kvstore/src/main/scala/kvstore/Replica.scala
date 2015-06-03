package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor, ActorLogging }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import context.system

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  
  // map of operation id -> (sender, actors expected to acknowledge)
  var replicatorAcks = Map.empty[Long, (ActorRef, Set[ActorRef])]

  // Persistence actor
  val persistence = system.actorOf(persistenceProps);
  
  // map of operation id -> persist msg
  var persistAcks = Map.empty[Long, (ActorRef, Persist)]
  
  // Restart any failed child
  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }
      
  // Resend unacknowledged persistence to persistence actor
  val timeout = Duration.create(100, MILLISECONDS);
  system.scheduler.schedule(timeout, timeout) {
    persistAcks.values foreach {
      case (client, msg) => persistence ! msg
    }
  }
  
  // After 1 second, if we haven't gotten a response from our persistence actor
  // and all of our replicators, send the client a failed message, and remove them
  // from the acknowledgement maps.
  def setFailTimer(id: Long) {
    system.scheduler.scheduleOnce(Duration.create(1, SECONDS)) {

      persistAcks get id foreach {
        case (client, msg) => {
          client ! OperationFailed(id)
          persistAcks -= id
        }
      }

      replicatorAcks get id foreach {
        case (client, actors) => {
          client ! OperationFailed(id)
          replicatorAcks -= id
        }
      }
    }
  }
  
  // Persist the given key / value, sending to persistence actor,
  // and adding an entry of expected acknowledgement in the form of
  // a Persisted message (see further below).
  def persist(key: String, valueOption: Option[String], id: Long) = {
    val persistMsg = Persist(key, valueOption, id)
    persistAcks += (id -> (sender, persistMsg))
    persistence ! persistMsg
  }
  
  def replicate (client: ActorRef, key: String, valueOption: Option[String], id: Long) = {
    if (! replicators.isEmpty) {
      replicatorAcks += (id -> (client, replicators))
      replicators foreach {
        repl => repl ! Replicate(key, valueOption, id)
      }
    }
  }
  
  
  // When a repliator is acknowledged, remove it from the expected
  // responding actors for this operation. If it was the last, remove
  // this operation from the acknowledgement map and send the client
  // an OperationAck message.
  def replicatorAcknowledged(id: Long, actor: ActorRef) = {

    replicatorAcks get id foreach {
      case (client, actors) => {
        val remaining = actors - actor
        if (remaining.isEmpty) {
          replicatorAcks -= id
        } else {
          replicatorAcks += (id -> (client, remaining))
        }
        
        // If we're not waiting on replicators or persistence, send ack to client              
        if (remaining.isEmpty && (persistAcks get id).isEmpty) {
          client ! OperationAck(id)
        }
      }
    }
  }

  // Switch behavior according to how we joined the cluster
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case Insert(key, value, id) => {
      log.debug("Insert " + id)
      setFailTimer(id)
      persist(key, Some(value), id)
      replicate(sender, key, Some(value), id)
    }
       
    case Remove(key, id) => {
      log.debug("Remove " + id)
      setFailTimer(id)
      persist(key, None, id)
      replicate(sender, key, None, id)
    }
    
    // Acknowledge of persistence
    case Persisted(key, id) => {
      log.debug("Primary persisted " + id)
      
      // Pull out this acknowledgement, since it's done
      val ack = persistAcks get id
      persistAcks -= id

      ack foreach {
        case (client, Persist(key, valueOption, id)) => {
          
          // It's persisted, now reflect the change locally
          valueOption match {
            case Some(value) => kv += (key -> value)
            case None => kv -= key
          }
        }
        
        // If we're not waiting on replicators or persistence, send ack to client              
        if ((replicatorAcks get id).isEmpty) {
          client ! OperationAck(id)
        }
      } 
    }
    
    case Replicated(key, id) => {
      log.debug("Replicated " + id)
      replicatorAcknowledged(id, sender)
    }
    
    case Get(key, id) => {
      log.debug("Get " + id)
      sender ! GetResult(key, kv get key, id)
    }

    case Replicas(replicas) => {
      log.debug("replicas " + replicas)
      
      val newSecondaries = replicas - self -- secondaries.keySet
      val removedSecondaries = secondaries.keySet -- replicas
      val removedReplicators = removedSecondaries map {
        sec => (secondaries get sec).get
      }
      
      // Remove all removed secondaries from our map
      removedSecondaries foreach {
        secondary => secondaries -= secondary
      }
      
      // Stop removed replicators and remove it from every entry in the replicatorAcks map
      removedReplicators foreach {
        replicator => {
          system.stop(replicator)
          replicatorAcks.keySet foreach {
            id => replicatorAcknowledged(id, replicator)
          }
        }
      }
      
      // Create replicators for our new secondaries and add them to the map 
      newSecondaries foreach { 
        secondary => {
          
          // Create replicator and add to the map
          val replicator = system.actorOf(Replicator.props(secondary))
          secondaries += (secondary -> replicator)
           
          // Send current state to new replicator
          kv.keys map {
            key => {
              replicator ! Replicate(key, kv get key, 0L)
            }
          }
        }
      }

      // Set new replicators set
      replicators = secondaries.values.toSet
    }
  }

  
  // Expected sequence number
  var expectedSeq = 0L
  
  // Map of key -> (sequence, pendingChange to the local key value store)
  var pendingChanges = Map.empty[String, (Long, Option[String])]
  
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    
    case Get(key, id) => {
      log.debug("Secondary Get")
      val effectiveValue = pendingChanges get key match {
        case Some((seq, valueOption)) => valueOption
        case _ => kv get key
      }
      sender ! GetResult(key, effectiveValue, id)
    }
    
    case Snapshot(key, valueOption, seq) => {
      log.debug("Snapshot, seq = " + seq + ", expected = " + expectedSeq)
      
      if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      }
      
      if (seq == expectedSeq) {
        
        // Make this a pending change
        pendingChanges += (key -> (seq, valueOption))
          
        // Then persist it
        log.debug("Secondary persisting...")
        persist(key, valueOption, seq)
      }
    }
    
    
    case Persisted(key, seq) => {
      log.debug("Secondary persisted, seq = " + seq + "")
      
      // Remove from acknowledgement map
      val ack = persistAcks get seq
      persistAcks -= seq
      
      ack foreach {
        case (client, Persist(key, valueOption, id)) => {
                  
          log.debug("Secondary persisted " + id)
          
          // Go ahead and modify local map
          valueOption match {
            case Some(value) => kv += (key -> value)
            case None => kv -= key
          }
          
          // Update expected sequence number
          expectedSeq = Math.max(expectedSeq, seq + 1)
          
          log.debug("New expected = " + expectedSeq)
          
          // If the current pending change has sequence <= ours, remove it
          pendingChanges get key foreach {
            case (pendingSeq, pendingChange) if (pendingSeq <= seq) => {
              pendingChanges -= key
            }
          }
          
          // Tell the client we're good
          client ! SnapshotAck(key, seq) 
        }
      }
    }
  }

  arbiter ! Join
}

