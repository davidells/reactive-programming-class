package calculator

import Math.sqrt;
import Math.pow;

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double], c: Signal[Double]): Signal[Double] =
    Signal( pow(b(), 2) - (4 * a() * c()) )

  def computeSolutions(a: Signal[Double], b: Signal[Double], 
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] =       
    Signal(delta() match {
      case d if d < 0 => Set()
      case d => Set(
          (-b() + sqrt(d)) / (2*a()),
          (-b() - sqrt(d)) / (2*a()))
    })
}
