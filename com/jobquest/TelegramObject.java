package com.jobquest;

public class TelegramObject {

    private long linevalue;
    private int maxlen;
    private String str;

    public TelegramObject(long lval, int stmaxlen, String st){
        this.linevalue = lval;
        this.maxlen = stmaxlen;
        this.str = st;
    }

    public long getLinevalue(){
        return this.linevalue;
    }

    public int getMaxlen(){
        return this.maxlen;
    }

    public String getString(){
        return this.str;
    }
}
