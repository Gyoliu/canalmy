package com.example.demo.entity;

/**
 * Created by liuxing on 2017/11/29.
 * 需要发送消息给kafka 一定要在里定义一个表枚举
 * 这个数据数据库表和java类得一个映射关系
 * 表得字段一定要和数据得字段相对应
 */
public enum Table {

    SKU(Sku.class),
    SMT_ADMIN(SmtAdmin.class)
    ;

    private Class cls;
    Table(Class cls){
        this.cls = cls;
    }

    public Class getCls() {
        return cls;
    }

    public void setCls(Class cls) {
        this.cls = cls;
    }
}
