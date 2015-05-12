package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] =
    namedExpressions map {
      case(k, v) => (k, Signal(eval(v(), namedExpressions)))
    }

  def eval(expr: Expr, refs: Map[String, Signal[Expr]]): Double = {
    expr match {
      case Literal(v) => v
      case Plus(a, b) => eval(a, refs) + eval(b, refs)
      case Minus(a, b) => eval(a, refs) - eval(b, refs)
      case Times(a, b) => eval(a, refs) * eval(b, refs)
      case Divide(a, b) => eval(a, refs) / eval(b, refs)
      case Ref(name) if refs contains name =>
        eval(getReferenceExpr(name, refs), refs - name)
      case _ => Double.NaN 
    }
  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
