package cn.edu360.shangguigu.hanshunping.scala.chapter17.abstractfactory.pizzastore.pizza

class PepperPizza extends Pizza{
  override def prepare(): Unit = {
    this.name = "胡椒pizza"
    println(this.name + " preparing")
  }
}
