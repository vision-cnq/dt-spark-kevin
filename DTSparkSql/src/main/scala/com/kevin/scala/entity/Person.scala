package com.kevin.scala.entity

/**
  * 自定义类要实现序列化接口
  * 自定义类访问级别必须是public
  * RDD转成DataFrame会把自定义类中字段的名称按assci排序
  */
class Person extends Serializable {

  // 默认拥有setget方法
  var id : String = _
  var name : String = _
  var age : Int = _

  def this(id:String,name:String,age:Int) {
    this()
    this.id=id
    this.name=name
    this.age=age
  }

  override def toString: String = {
    "Person [id="+id+", name="+name+", age="+age+"]"
  }

}
