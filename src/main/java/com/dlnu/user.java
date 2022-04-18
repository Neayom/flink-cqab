package com.dlnu;

/**
 * Created by lgx on 2022/4/17.
 */
public class user {
    @Override
    public String toString() {
        return "user1{}";
    }

    public user() {
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    @Override
    public boolean equals(Object obj) {
        System.out.println("zhangsan");
        return super.equals(obj);
    }
}
