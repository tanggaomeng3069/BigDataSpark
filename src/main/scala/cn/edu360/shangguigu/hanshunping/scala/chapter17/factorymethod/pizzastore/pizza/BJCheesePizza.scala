package cn.edu360.shangguigu.hanshunping.scala.chapter17.factorymethod.pizzastore.pizza

class BJCheesePizza extends Pizza{
  override def prepare(): Unit = {
    this.name = "北京奶酪Pizza"
    println(this.name + " preparing..")
  }
}
