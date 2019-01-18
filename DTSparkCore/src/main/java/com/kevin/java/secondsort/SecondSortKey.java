package com.kevin.java.secondsort;

import java.io.Serializable;

/**
 * @author kevin
 * @version 1.0
 * @description     序列化并重写排序方法
 * @createDate 2019/1/2
 */
public class SecondSortKey implements Serializable,Comparable<SecondSortKey> {
    private int first;
    private int second;
    public int getFirst() {
        return first;
    }
    public void setFirst(int first) {
        this.first = first;
    }
    public int getSecond() {
        return second;
    }
    public void setSecond(int second) {
        this.second = second;
    }
    public SecondSortKey(int first, int second) {
        super();
        this.first = first;
        this.second = second;
    }
    // 重写排序方法,降序
    @Override
    public int compareTo(SecondSortKey o1) {
        if(getFirst() - o1.getFirst() == 0 ){
            return getSecond() - o1.getSecond();
        }else{
            return getFirst() - o1.getFirst();
        }
    }
}
