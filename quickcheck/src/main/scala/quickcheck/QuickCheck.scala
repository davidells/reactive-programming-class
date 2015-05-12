package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }
  
  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, empty)
    val h2 = insert(b, h)
    findMin(h2) == Math.min(a, b)
  }
  
  property("del1") = forAll{ a: Int =>
    val h = insert(a, empty)
    deleteMin(h) == empty
  }
  
  property("meld1") = forAll{ (h1: H, h2: H) =>
    val m = if (isEmpty(h1)) 0 else findMin(h1)
    val m2 = if (isEmpty(h2)) 0 else findMin(h2)
    findMin(meld(h1, h2)) == Math.min(m, m2)
  }
  
  property("minseq1") = forAll{ (list: List[Int]) =>
    val h = insertAll(empty, list)
    heapMatchesList(h, list)
  }
  
  private def insertAll(h: H, xs: List[Int]): H = {
		  if (xs.isEmpty) h else insertAll(insert(xs.head, h), xs.tail)
  }
  
  private def heapMatchesList(h: H, xs: List[Int]): Boolean = {
    if (xs.isEmpty) isEmpty(h) else heapMatchesList_r(h, xs.sorted)
  }
  
  private def heapMatchesList_r(h: H, sortedXs: List[Int]): Boolean = sortedXs match {
    case Nil => isEmpty(h)
    case head :: tail => 
      findMin(h) == head && heapMatchesList_r(deleteMin(h), tail)
  }
  

  lazy val genHeap: Gen[H] = for {
    v <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(v, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
