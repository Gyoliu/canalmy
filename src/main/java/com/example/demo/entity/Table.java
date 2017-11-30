package com.example.demo.entity;

/**
 * Created by liuxing on 2017/11/29.
 */
public enum Table {

    SKU(Sku.class)

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
