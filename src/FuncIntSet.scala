trait FuncSetOperations {

  type FunSet[Int] = Int => Boolean

  def contains(set: FunSet[Int], elem: Int): Boolean

  def singletonSet(elem: Int): FunSet[Int]

  def union(set: FunSet[Int], another: FunSet[Int]): FunSet[Int]

  def intersect(set: FunSet[Int], another: FunSet[Int]): FunSet[Int]

  def diff(set: FunSet[Int], another: FunSet[Int]): FunSet[Int]

  def filter(set: FunSet[Int], f: Int => Boolean): FunSet[Int]

  def map(set: FunSet[Int], f: Int => Int, elements: Int*): FunSet[Int]

  def forAll(set: FunSet[Int], f: Int => Boolean): Boolean

  def exists(set: FunSet[Int], f: Int => Boolean): Boolean

  def asString(set: FunSet[Int]): String

}

object FuncSetImpl extends FuncSetOperations {
  override def contains(set: FuncSetImpl.FunSet[Int], elem: Int): Boolean = set(elem)

  override def singletonSet(elem: Int): FuncSetImpl.FunSet[Int] = (e: Int) => e == elem

  override def union(set: FuncSetImpl.FunSet[Int], another: FuncSetImpl.FunSet[Int]): FuncSetImpl.FunSet[Int] =
    (elem: Int) => set(elem) || another (elem)

  override def intersect(set: FuncSetImpl.FunSet[Int], another: FuncSetImpl.FunSet[Int]): FuncSetImpl.FunSet[Int] =
    (elem: Int) => set(elem) && another (elem)

  override def diff(set: FuncSetImpl.FunSet[Int], another: FuncSetImpl.FunSet[Int]): FuncSetImpl.FunSet[Int] =
    (elem: Int) => set(elem) && !another(elem)

  override def filter(set: FuncSetImpl.FunSet[Int], f: Int => Boolean): FuncSetImpl.FunSet[Int] =
    (elem: Int) => contains(set, elem) && f(elem)

  override def forAll(set: FuncSetImpl.FunSet[Int], f: Int => Boolean): Boolean = {
    def loop (elem: Int): Boolean = {
      if (contains(set, elem) && !f(elem)) false
      else if (Int.MinValue > elem || Int.MaxValue < elem) false
      else loop (elem+1)
    }
    loop (Int.MinValue)
  }

  override def exists(set: FuncSetImpl.FunSet[Int], f: Int => Boolean): Boolean = {
    def loop (elem: Int): Boolean = {
      if (contains(set, elem) && f(elem)) true
      else if (Int.MinValue > elem || Int.MaxValue < elem) false
      else loop (elem+1)
    }
    loop (Int.MinValue)
  }

  override def map(set: FuncSetImpl.FunSet[Int], f: Int => Int): FuncSetImpl.FunSet[Int] = {
    (elem: Int) => (exists(set, e => f(e) == elem))
  }

  override def asString(set: FuncSetImpl.FunSet[Int]): String = {
    val str = for (elem <- Int.MinValue to Int.MaxValue if contains(set, elem)) yield elem
    str.mkString("{", ",", "}")
  }
}
