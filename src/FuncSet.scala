trait FuncSetOperations {

  type FunSet[A] = A => Boolean

  def contains[A](set: FunSet[A], elem: A): Boolean

  def singletonSet[A](elem: A): FunSet[A]

  def union[A](set: FunSet[A], another: FunSet[A]): FunSet[A]

  def intersect[A](set: FunSet[A], another: FunSet[A]): FunSet[A]

  def diff[A](set: FunSet[A], another: FunSet[A]): FunSet[A]

  def filter[A](set: FunSet[A], f: A => Boolean): FunSet[A]

  def map[A, B](set: FunSet[A], f: A => B, elements: A*): FunSet[B]

  def forAll[A](set: FunSet[A], f: A => Boolean, elements: A*): Boolean

  def exists[A](set: FunSet[A], f: A => Boolean, elements: A*): Boolean

  def asString[A](set: FunSet[A], elements: A*): String

}

object FuncSetImpl extends FuncSetOperations {
  override def contains[A](set: FuncSetImpl.FunSet[A], elem: A): Boolean = set(elem)

  override def singletonSet[A](elem: A): FuncSetImpl.FunSet[A] = (e: A) => e == elem

  override def union[A](set: FuncSetImpl.FunSet[A], another: FuncSetImpl.FunSet[A]): FuncSetImpl.FunSet[A] =
    (elem: A) => set(elem) || another (elem)

  override def intersect[A](set: FuncSetImpl.FunSet[A], another: FuncSetImpl.FunSet[A]): FuncSetImpl.FunSet[A] =
    (elem: A) => set(elem) && another (elem)

  override def diff[A](set: FuncSetImpl.FunSet[A], another: FuncSetImpl.FunSet[A]): FuncSetImpl.FunSet[A] =
    (elem: A) => set(elem) && !another(elem)

  override def filter[A](set: FuncSetImpl.FunSet[A], f: A => Boolean): FuncSetImpl.FunSet[A] =
    (elem: A) => contains(set, elem) && f(elem)

  override def forAll[A](set: FuncSetImpl.FunSet[A], f: A => Boolean, elements: A*): Boolean = {
    def loop (pos: Int): Boolean = {
      if (pos >= elements.size) true
      else f(elements.apply(pos)) && loop(pos+1)
    }
    loop (0)
  }

  override def exists[A](set: FuncSetImpl.FunSet[A], f: A => Boolean, elements: A*): Boolean = {
    def loop (pos: Int): Boolean = {
      if (pos >= elements.size) false
      else f(elements.apply(pos)) || loop(pos+1)
    }
    loop (0)
  }

  override def map[A, B](set: FuncSetImpl.FunSet[A], f: A => B, elements: A*): FuncSetImpl.FunSet[B] =
    (n_elem: B) => exists(set, (elem: A) => f(elem) == n_elem, elements)

  override def asString[A](set: FuncSetImpl.FunSet[A], elements: A*): String = {
    elements.mkString("{", ",", "}")
  }
}
