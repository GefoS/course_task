//1) Найти K-тый элемент в списке
def getKElem (list: List[Int], k: Int): Option[Int] = (list, k) match {
  case (head::_, 0) => Some (head)
  case (_::tail, k) => getKElem(tail, k-1)
  case _ => None
  /*if (k == 0) Some(list.head)
  else if (list.isEmpty) None
  else getKElem(list.tail, k-1)*/
}
val a = getKElem(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

//2) Получить число элементов списка без использования size и length
def len (list: List[Int]) = list.fold(0)((a, _) => a + 1)
val b = len(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

val lp = List(1, 2, 3, 2, 1)
//3) Написать функцию проверки последовательности на палиндромность (ex: 1 2 3 2 1)
def isPalindrome (list: List[Int]): Boolean = list match {
  case List(a) => true
  case list => (list.head == list.last && isPalindrome (list.tail.init))
}
val c = isPalindrome(lp)


//4) Написать функцию distinct по удалению дублей из последовательности. Set и аналоги использовать нельзя
val dups = List(1, 4, 4, 2, 1, 2, 4, 5, 4, 2, 7, 8, 7, 9, 0, 1)
/*def distinct (list:List[Int] ): List[Int] = {
  def removeDup (list:List[Int], elem:Int): List[Int] ={
    list.filter(e => e != elem) :+ elem
  }
  //?
}*/
//val new_dups = compress(dups)

//6) Написать собственную реализацию slice(Option[StartPos], Option[EndPos])
def slice[A] (list: List [A], start: Option[Int], end: Option[Int]): List[A] = {
  def cut[A] (l: List [A], s: Int): List[A] = {
    if (s <= 0 || l.isEmpty) l
    else cut(l.tail, s-1)
  }
  (list, start, end) match {
    case (List(), _, _) | (List(_), _, _)=> list
    case (l, s, None) => cut (l, s.get)
    case (l, None, e) => l diff cut (l, e.get)
    case (l, s, e) => cut((l diff cut (l, e.get)), s.get)
  }
}
var n2 = slice (List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Some(2), None)
var n3 = slice (List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), None, Some(5))
var n4 =  slice (List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Some(2), Some(5))

//8) Написать функцию выводящую только простые числа из заданого диапазона(Range)
def getPrimeRange (range: Range): List[Int] = {
  def isPrime (num: Int): Boolean ={
    if (num == 1 || num == 2) true
    else {
      for (i <- 2 until num){
        if (num % i == 0) return false
      }
      true
    }
  }
  range.toList.filter(e => isPrime(e))

}
val z = getPrimeRange(1 to 228)

//7) Написать функцию создающую из заданой последовательности (любого типа)
//все возможные группы по N и только уникальные. (1, 2) и (2, 1) считаются одинаковыми комбинациями.
/*val r = (1 to 3).toList
def cross[A] (list: List[A]): List[(A, A)] = {
  def product (list: List[Int]): (A, A) = list match {
    case List(a) => (a, a)
    case
  }
}*/

/*
 //5) Написать distinctPack дающий результат вида (уникальная последовательность, список с вложенными списками из дубликатов)
  //(ex: input - [1, 3, 3, 2, 1] -> ([[1, 1], [3, 3], [2]]))
