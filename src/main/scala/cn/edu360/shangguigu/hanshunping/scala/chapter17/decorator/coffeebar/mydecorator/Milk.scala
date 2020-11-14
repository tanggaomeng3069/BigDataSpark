package cn.edu360.shangguigu.hanshunping.scala.chapter17.decorator.coffeebar.mydecorator

import cn.edu360.shangguigu.hanshunping.scala.chapter17.decorator.coffeebar.Drink


class Milk(obj: Drink) extends Decorator(obj) {

  setDescription("Milk")
  setPrice(2.0f)
}
