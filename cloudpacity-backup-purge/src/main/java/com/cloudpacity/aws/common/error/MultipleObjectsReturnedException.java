// Decompiled by Jad v1.5.8e. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.geocities.com/kpdus/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   MultipleObjectsReturnedException.java

package com.cloudpacity.aws.common.error;


// Referenced classes of package com.cloudpacity.aws.common.error:
//            CPRuntimeException

public class MultipleObjectsReturnedException extends CPRuntimeException
{

    public MultipleObjectsReturnedException()
    {
    }

    public MultipleObjectsReturnedException(String errorMsg)
    {
        super(errorMsg);
    }

    private static final long serialVersionUID = 0x1f7d5b657addb976L;
}
