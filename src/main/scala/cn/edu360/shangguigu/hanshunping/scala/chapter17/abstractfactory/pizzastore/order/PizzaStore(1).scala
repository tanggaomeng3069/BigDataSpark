package cn.edu360.shangguigu.hanshunping.scala.chapter17.abstractfactory.pizzastore.order

object PizzaStore {
  def main(args: Array[String]): Unit = {
    new OrderPizza(new BJFactory)
    //new OrderPizza(new LDFactory)
  }
}
