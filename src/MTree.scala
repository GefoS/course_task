import scala.collection.mutable.ListBuffer

sealed trait Tree[+A]

case class Leaf[A](value: A) extends Tree[A]

case class Branch[A](left: Tree[A], right: Tree[A]) extends Tree[A]

object Tree {
  implicit class TreeImpl[T] (tree: Tree[T]) {
    //leaf and branch count
    def size: Int = tree match {
      case Leaf(_) => 1
      case Branch(left, right) => left.size + right.size + 1
    }

    //leaf counts
    def count: Int = tree match {
      case Leaf(_) => 1
      case Branch(left, right) => left.size + right.size
    }

    def depth: Int = tree match {
      case Leaf(_) => 1
      case Branch(left, right) => math.max(left.depth, right.depth)
    }

    def map[N](f: T => N): Tree[N] = tree match {
      case Leaf(value) => Leaf(f(value))
      case Branch(left, right) => Branch (left.map(f), right.map(f))
    }

    def filter(f: T => Boolean): Tree[T] = {
      def rebuild (r_tree: Tree[Option[T]]): Tree[Option[T]] = r_tree match {
        case Leaf (value) => if (f(value.get)) Leaf(value) else Leaf(None)
        case Branch (left, right) => left.
      }
    }

    //case Cons(h, t) => t.foldLeft(f(acc, h))(f)
    def fold[N] (map: T => N) (reduce: (N, N) => N):N = tree match {
      case Branch(left, right) => reduce(left.fold()(), right.fold()())
      case Leaf (value) => map (value)
    }
  }
}
