package cn.edu360.shangguigu.hanshunping.scala.chapter17.decorator.coffeebar.mydecorator

import cn.edu360.shangguigu.hanshunping.scala.chapter17.decorator.coffeebar.Drink



class NewMilk(obj: Drink) extends Decorator(obj) {

  setDescription("新式Milk")
  setPrice(4.0f)
}