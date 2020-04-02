package spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Customer, Order, Product}
import java.sql.Date

object Lab3Join extends App {
  /*
   * Lab3 - пример использования join
   * Расчитать кто и сколько сделал заказов, за какие даты, на какую сумму.
   * Итоговое множество содержит поля: customer.name, order.order.orderDate, sum(order.numberOfProduct * product.price)
   *
   * 1. Создать экземпляр класса SparkConf
   * 2. Установить мастера на local[*] и установить имя приложения
   * 3. Создать экземпляр класса SparkContext, используя объект SparkConf
   * 4. Загрузить в RDD файл src/test/resources/input/order
   * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить строки RDD
   * 6. Выбрать ключ поле (customerID), в значение (orderDate, numberOfProduct, productID)
   * 7. Загрузить в RDD файл src/test/resources/input/customer
   * 8. Используя класс [[ru.phil_it.bigdata.entity.Customer]], распарсить строки RDD
   * 9. Выбрать ключ поле (id), в значение (name)
   * 10. Выполнить внутреннее соединение RDD из п.6 и п.9
   * 11. Выбрать ключ (productID), в значение (customer.name, orderDate,  numberOfProduct)
   * 12. Загрузить в RDD файл src/test/resources/input/product
   * 13. Используя класс [[ru.phil_it.bigdata.entity.Product]], распарсить строки RDD
   * 14. Выбрать ключ (id), значение (price)
   * 15. Выполнить внутреннее соединение с RDD из п.11 и п.14
   * 16. Выбрать ключ (customer.name, order.orderDate), значение (order.numberOfProduct * product.price)
   * 17. Расчитать сумму в значении по ключу
   * 18. Вывести результат или записать в директорию src/test/resources/output/lab3
   * */

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val orders: RDD[(Int, (Date, Int, Int))] = sc.textFile(Parameters.path_order)
    .map(str => Order(str))
    .map(or => (or.customerID, (or.orderDate, or.numberOfProduct, or.productID)))

  val customers: RDD[(Int, String)] = sc.textFile(Parameters.path_customer)
    .map(str => Customer(str))
    .map(cus => (cus.id, cus.name))

  //(CUS_ID, ((DATE, NUM, PR_ID), CUS_NAME))
  //Выбрать ключ (productID), в значение (customer.name, orderDate,  numberOfProduct)
  val customerOrders = orders
    .join(customers)
    .map {
      case (_, (order, customer)) => (order._3, (customer, order._1, order._2))
    }

  //16. Выбрать ключ (customer.name, order.orderDate), значение (order.numberOfProduct * product.price)
  val customerSumProducts = sc.textFile(Parameters.path_product)
    .map(str => Product(str))
    .map(pr => (pr.id, pr.price))
    .join(customerOrders)
    .map {
      case (_, (pr, cs_or)) => ((cs_or._1, cs_or._2), pr * cs_or._3)
    }
    .reduceByKey(_ + _)
    .saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab3")

  sc.stop()
}
