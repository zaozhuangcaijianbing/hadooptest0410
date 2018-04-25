package com.wn.spark;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class KafkaMessage implements Serializable {

    private static final long serialVersionUID = -4390616343881606728L;
    private String ak;
    private String udid;
    private String op;
    private String ts;
    private JSONObject param;
    private String uid;
    private String sid;
    private String ip;
    private String av;
    private String dm;
    private String ch;


    private String ifa;

    public String getDm() {
        return dm;
    }

    public void setDm(String dm) {
        this.dm = dm;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }


    public String getAk() {
        return ak;
    }

    public void setAk(String ak) {
        this.ak = ak;
    }

    public String getUdid() {
        return udid;
    }

    public void setUdid(String udid) {
        this.udid = udid;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public JSONObject getParam() {
        return param;
    }

    public void setParam(JSONObject param) {
        this.param = param;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }


    public String getIfa() {
        return ifa;
    }

    public void setIfa(String ifa) {
        this.ifa = ifa;
    }

    public String getCh() {
        return ch;
    }

    public void setCh(String ch) {
        this.ch = ch;
    }

    public String getAB() {
    		return "A";
    }

    public JSONArray getData() {
        String data = getParamByKey("DATA");
        JSONArray jsonArray = null;
        try {
            jsonArray = JSONObject.parseArray(data);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(this);
        }
        return jsonArray;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "ak='" + ak + '\'' +
                ", udid='" + udid + '\'' +
                ", op='" + op + '\'' +
                ", ts='" + ts + '\'' +
                ", param=" + param +
                ", uid='" + uid + '\'' +
                ", sid='" + sid + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }

    public String getParamByKey(String key) {
        if (null == param || !param.containsKey(key))
            return null;

        return param.getString(key);
    }

    public String getAv() {
        return av;
    }

    public void setAv(String av) {
        this.av = av;
    }
}
