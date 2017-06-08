package com.cloudpacity.aws.purge.pojo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class BackupPurgeRequest {

	
	private String awsAccountId;
    private ZonedDateTime requestStartTime;
    private String originatingLambdaRequestId;
    private String currentLambdaRequestId;


	public String getAwsAccountId() {
		return awsAccountId;
	}

	public void setAwsAccountId(String awsAccountId) {
		this.awsAccountId = awsAccountId;
	}
	
    
	public String getOriginatingLambdaRequestId()
    {
        return originatingLambdaRequestId;
    }

    public void setOriginatingLambdaRequestId(String originatingLambdaRequestId)
    {
        this.originatingLambdaRequestId = originatingLambdaRequestId;
    }

    public String getCurrentLambdaRequestId()
    {
        return currentLambdaRequestId;
    }

    public void setCurrentLambdaRequestId(String currentLambdaRequestId)
    {
        this.currentLambdaRequestId = currentLambdaRequestId;
    }	
	
    public ZonedDateTime getRequestStartTime()
    {
        return requestStartTime;
    }

    public void setRequestStartTime(ZonedDateTime requestStartTime)
    {
        this.requestStartTime = requestStartTime;
    }	
    public void setRequestStartTimeString(String dateString)
    {
        if(dateString != null)
        {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ss.SSS-z");
            LocalDateTime datetime = LocalDateTime.parse(dateString, dateFormatter);
            ZoneId zone = ZoneId.of("America/Chicago");
            requestStartTime = ZonedDateTime.of(datetime, zone);
        }
    }    
    public String getRequestStartTimeString()
    {
        if(getRequestStartTime() != null)
        {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ss.SSS-z");
            return requestStartTime.format(dateFormatter);
        } else
        {
            return "";
        }
    }
}