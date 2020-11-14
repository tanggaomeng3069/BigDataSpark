package cn.edu360.shangguigu.hanshunping.scala.chapter17.abstractfactory.pizzastore.order

import cn.edu360.shangguigu.hanshunping.scala.chapter17.abstractfactory.pizzastore.pizza.Pizza

trait AbsFactory {

  //一个抽象方法
  def  createPizza(t : String ): Pizza

}
