package com.kevin.scala.secondsort

/**
  * 自定义排序,sorted排序继承Ordered,需要序列化
  */
class SecondSortKey(val first:Int,val second:Int) extends Ordered[SecondSortKey] with Serializable {
  // 重写排序方法,降序
  override def compare(that: SecondSortKey): Int = {
    if (this.first - that.first == 0) {
      this.second - that.second
    } else {
      this.first - that.first
    }
  }

}
