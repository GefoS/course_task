package spark.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.Order

object Lab6AggregateBy extends App {
  /*
   * Lab6 - пример использования aggregateByKey
   * Определить кол-во уникальных заказов, максимальный объем заказа,
   * минимальный объем заказа, общий объем заказа за всё время
   * Итоговое множество содержит поля: order.customerID, count(distinct order.productID),
   * max(order.numberOfProduct), min(order.numberOfProduct), sum(order.numberOfProduct)
   *
   * 1. Создать экземпляр класса SparkConf
   * 2. Установить мастера на local[*] и установить имя приложения
   * 3. Создать экземпляр класса SparkContext, используя объект SparkConf
   * 4. Загрузить в RDD файл src/test/resources/input/order
   * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить строки в RDD
   * 6. Выбрать только те транзакции у которых статус delivered
   * 7. Выбрать ключ (customerID), значение (productID, numberOfProducts)
   * 8. Создать кейс класс Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)
   * 9. Создать аккумулятор с начальным значением
   * 10. Создать ананимную функцию для заполнения аккумульятора
   * 11. Создать ананимную функцию для слияния аккумуляторов
   * 12. Выбрать id заказчика, размер коллекции uniqProducts,
   *   максимальное и минимальное значение из uniqNumOfProducts и sumNumOfProduct
   * 13. Вывести результат или записать в директорию src/test/resources/output/lab6
   * */

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test-app")
  val sc = new SparkContext(sparkConf)

  val orders: RDD[(Int, (Int, Int))] = sc.textFile(Parameters.path_order)
    .map(str => Order(str))
    .filter(or => or.status.equals("delivered"))
    .map(or => (or.customerID, (or.productID, or.numberOfProduct)))

  case class Result(
                     uniqueProducts: Set[Int],
                     maxProductsVolume: Int,
                     minProductsVolume: Int,
                     productsSumByCustomers: Int
                   )

  def getUniqueProduct = (acc: Set[Int], elem: (Int, Int)) =>
    if (acc.contains(elem._1)) acc else acc.+(elem._1)

  def combinationUniqueOperation = (acc1: Set[Int], acc2: Set[Int]) => acc1 ++ acc2

  def getMaxProductVolume = (acc: Int, elem: (Int, Int)) =>
    if (acc > elem._2) acc else elem._2

  def getMinProductVolume = (acc: Int, elem: (Int, Int)) =>
    if (acc < elem._2) acc else elem._2

  def combinationMaxOperation = (acc1: Int, acc2: Int) =>
    if (acc1 > acc2) acc1 else acc2

  def combinationMinOperation = (acc1: Int, acc2: Int) =>
    if (acc1 < acc2) acc1 else acc2

  def productsSumByCustomer = (acc: Int, elem: (Int, Int)) => acc + elem._2

  def combinationSumOperation = (acc1: Int, acc2: Int) => acc1 + acc2


  val initialSet: Set[Int] = Set.empty

  val countUniqueOrders = orders
    .aggregateByKey(initialSet)(getUniqueProduct, combinationUniqueOperation)
    .map {
      case (k, v) => (k, v.size)
    }

  val maxProductsVolume:RDD[(Int, Int)] = orders.aggregateByKey(0)(getMaxProductVolume, combinationMaxOperation)
  val minProductsVolume = orders.aggregateByKey(Int.MaxValue)(getMinProductVolume, combinationMinOperation)
  val productsSumByCustomers = orders.aggregateByKey(0)(productsSumByCustomer, combinationSumOperation)

}
