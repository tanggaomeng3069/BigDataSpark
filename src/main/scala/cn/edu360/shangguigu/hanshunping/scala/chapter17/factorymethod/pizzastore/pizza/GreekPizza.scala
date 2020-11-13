package cn.edu360.shangguigu.hanshunping.scala.chapter17.factorymethod.pizzastore.pizza

class GreekPizza extends Pizza {
  override def prepare(): Unit = {
    this.name = "希腊pizza"
    println(this.name + " preparing")
  }
}
