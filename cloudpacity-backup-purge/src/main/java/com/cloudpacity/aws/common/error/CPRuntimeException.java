// Decompiled by Jad v1.5.8e. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.geocities.com/kpdus/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   CPRuntimeException.java

package com.cloudpacity.aws.common.error;


public class CPRuntimeException extends RuntimeException
{

    public CPRuntimeException()
    {
        errorMsg = "";
    }

    public CPRuntimeException(String errorMsg)
    {
        this.errorMsg = "";
        this.errorMsg = errorMsg;
    }

    public String getErrorMessage()
    {
        return errorMsg;
    }

    public String getMessage()
    {
        if(errorMsg.equalsIgnoreCase(""))
            return super.getMessage();
        if(super.getMessage() != null)
            return (new StringBuilder(String.valueOf(errorMsg))).append(";  ").append(super.getMessage()).toString();
        else
            return errorMsg;
    }

    private static final long serialVersionUID = 0xea1c3184778d85d5L;
    private String errorMsg;
}
