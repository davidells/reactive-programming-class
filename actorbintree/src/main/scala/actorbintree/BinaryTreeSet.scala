/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case msg: Operation => root ! msg
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished => {
      while (! pendingQueue.isEmpty) {
        val (operation, newQueue) = pendingQueue.dequeue
        newRoot ! operation
        pendingQueue = newQueue
      }
      root ! PoisonPill
      root = newRoot
      context.become(normal)
    }

    case msg: Operation => pendingQueue = pendingQueue.enqueue(msg)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved
  
  var copyRepliesExpected = 0
  var copyRepliesReceived = 0

  // optional
  def receive = normal

  def newNode(elem: Int): ActorRef = {
    context.actorOf(BinaryTreeNode.props(elem, false));
  }
  
  def insertOrSendTo(position: Position, requester: ActorRef, id: Int, elem: Int): Unit = {
    if (subtrees.contains(position)) {
      subtrees(position) ! Insert(requester, id, elem)
    } else {
      subtrees = subtrees + (position -> newNode(elem))
      requester ! OperationFinished(id)
    }
  }
  
  def containsOrSendFalse(position: Position, requester: ActorRef, id: Int, elem: Int): Unit = {
    if (subtrees.contains(position)) {
      subtrees(position) ! Contains(requester, id, elem)
    } else {
      requester ! ContainsResult(id, false)
    }
  }
  
  def removeOrFinish(position: Position, requester: ActorRef, id: Int, elem: Int): Unit = {
    if (subtrees.contains(position)) {
      subtrees(position) ! Remove(requester, id, elem)
    } else {
      requester ! OperationFinished(id)
    }
  }
  
  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    
    case Insert(requester, id, elem) => {
      if (elem < this.elem) {
        insertOrSendTo(Left, requester, id, elem)
      } else if (elem > this.elem) {
        insertOrSendTo(Right, requester, id, elem)
      } else {
        removed = false
        requester ! OperationFinished(id)
      }
    }
    
    case Contains(requester, id, elem) => {
      if (elem < this.elem) {
        containsOrSendFalse(Left, requester, id, elem)
      } else if (elem > this.elem) {
        containsOrSendFalse(Right, requester, id, elem)
      } else {
        requester ! ContainsResult(id, !removed) 
      }
    }
    
    case Remove(requester, id, elem) => {
      if (elem < this.elem) {
        removeOrFinish(Left, requester, id, elem)
      } else if (elem > this.elem) {
        removeOrFinish(Right, requester, id, elem)
      } else {
        removed = true
        requester ! OperationFinished(id)
      }
    }
    
    case CopyTo(treeNode) => {
      copyRepliesExpected = 0
      
      if (! removed) {
        treeNode ! Insert(self, id=0, this.elem)
      }
      
      if (subtrees.contains(Left)) {
        copyRepliesExpected = copyRepliesExpected + 1
        subtrees(Left) ! CopyTo(treeNode)
      }
      
      if (subtrees.contains(Right)) {
        copyRepliesExpected = copyRepliesExpected + 1
        subtrees(Right) ! CopyTo(treeNode)
      }
      
      if (copyRepliesExpected == 0) {
        context.parent ! CopyFinished
      }
    }
    
    case CopyFinished => {
      copyRepliesReceived = copyRepliesReceived + 1
      if (copyRepliesReceived >= copyRepliesExpected) {
        context.parent ! CopyFinished
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
