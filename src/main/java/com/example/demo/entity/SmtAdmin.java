package com.example.demo.entity;

import com.alibaba.fastjson.JSON;

import java.util.Date;

/**
 * Created by liuxing on 2017/12/2.
 */
public class SmtAdmin {

    private Integer id;
    private String account;
    private String sales;
    private String sales_assistant;
    private String store_name;
    private Integer act_status;
    private Date fetch_time;
    private String sellerAdminSeq;

    public String toString(){
        return JSON.toJSONString(this);
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getSales() {
        return sales;
    }

    public void setSales(String sales) {
        this.sales = sales;
    }

    public String getSales_assistant() {
        return sales_assistant;
    }

    public void setSales_assistant(String sales_assistant) {
        this.sales_assistant = sales_assistant;
    }

    public String getStore_name() {
        return store_name;
    }

    public void setStore_name(String store_name) {
        this.store_name = store_name;
    }

    public Integer getAct_status() {
        return act_status;
    }

    public void setAct_status(Integer act_status) {
        this.act_status = act_status;
    }

    public Date getFetch_time() {
        return fetch_time;
    }

    public void setFetch_time(Date fetch_time) {
        this.fetch_time = fetch_time;
    }

    public String getSellerAdminSeq() {
        return sellerAdminSeq;
    }

    public void setSellerAdminSeq(String sellerAdminSeq) {
        this.sellerAdminSeq = sellerAdminSeq;
    }
}
