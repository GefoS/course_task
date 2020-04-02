package spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Customer, Order, Product}

object Lab4CompositeKey extends App{
  /*
 * Lab4 - пример использования cartesian и join по составному ключу
 * Расчитать кто и на какую сумму купил определенного товара за всё время
 * Итоговое множество содержит поля: customer.name, product.name, order.numberOfProduct * product.price
 *
 * 1. Создать экземпляр класса SparkConf
 * 2. Установить мастера на local[*] и установить имя приложения
 * 3. Создать экземпляр класса SparkContext, используя объект SparkConf
 * 4. Загрузить в RDD файлы src/test/resources/input/order
 * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить RDD
 * 6. Выбрать ключ (customerID, productID), значение (numberOfProduct)
 * 7. Загрузить в RDD файлы src/test/resources/input/product
 * 8. Используя класс [[ru.phil_it.bigdata.entity.Product]], распарсить RDD
 * 9. Загрузить в RDD файлы src/test/resources/input/customer
 * 10. Используя класс [[ru.phil_it.bigdata.entity.Customer]], распарсить RDD
 * 11. Выполнить перекрестное соединение RDD из п.8 и п.10
 * 12. Выбрать ключ (customer.id, product.id), значение (customer.name, product.name, prodcut.price)
 * 13. Выполнить левое соединение RDD из п.6 и п.13
 * 14. Поставить заглушку на результат соединения для левой таблицы ("default", "default", 0d)
 * 15. Выбрать поля custemer.name, product.name, order.numberOfProduct * product.price
 * 16. Вывести результат или записать в директорию src/test/resources/output/lab4
 * */

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val products: RDD[(Int, (String, Double))] = sc.textFile(Parameters.path_product)
    .map(str => Product(str))
    .map(pr => (pr.id, (pr.name, pr.price)))

  val orders: RDD[((Int, Int), Int)] = sc.textFile(Parameters.path_order)
    .map(str => Order(str))
    .map(or => ((or.customerID, or.productID), or.numberOfProduct))

  val customers: RDD[(Int, String)] = sc.textFile(Parameters.path_customer)
    .map(str => Customer(str))
    .map(cus => (cus.id, cus.name))

  //((1,(Apple iPhone 7,45990.0)),(1,John))
  //( ( PR_ID, (PR_NAME, PR_PRICE) ), (CUS_ID, CUS_NAME))
  val cartesianResult = products
    .cartesian(customers)
    .map{
      case (product, customer) => ((product._1, customer._1), (customer._2, product._2._1, product._2._2))
    }

  val result = orders
    .join(cartesianResult)
    .map{
      case (_, (pr_num, (cus_name, pr_name, price))) => (cus_name, pr_name, pr_num*price)
    }
    .saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab4")

}
