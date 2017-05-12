package com.cloudpacity.aws.common.util;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ArrayBlockingQueue;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
/**
 * 
 * Copyright 2015 Cloudpacity
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * @author Scott Wheeler
 *
 */
public class CPLogger
{
    public static final int DEFAULT_DEBUG_QUEUE_SIZE = 20;
    protected LambdaLogger logger;
    protected ZonedDateTime currDateTime;
    protected ZoneId zoneCDT;
    protected String timezone;
    public static final String DEFAULT_TIME_ZONE = "America/Chicago";
	protected final ArrayBlockingQueue<String> debugMessageQueue = new ArrayBlockingQueue<String>(DEFAULT_DEBUG_QUEUE_SIZE);
    protected String logMessages = "";
    protected String summaryLogMessages = "";

    
    public CPLogger(LambdaLogger logger)
    {
        this.logger = logger;
        this.timezone = DEFAULT_TIME_ZONE;
    }

    public CPLogger(LambdaLogger logger, String timezone)
    {
        this.timezone = timezone;
        this.logger = logger;
    }
    
    public void logSummary(String message)
    {
        summaryLogMessages = summaryLogMessages +  message + System.getProperty("line.separator");
    }

    public void log(String message)
    {
        zoneCDT = ZoneId.of(this.timezone);
        currDateTime = ZonedDateTime.now(zoneCDT);
        logger.log(currDateTime.toString() + ":  " + message);
        logMessages = logMessages + currDateTime.toString() + ": " + message + System.getProperty("line.separator");
    }

    public void logDebug(String message)
    {
        zoneCDT = ZoneId.of(this.timezone);
        currDateTime = ZonedDateTime.now(zoneCDT);
        addDebugMessage(message);
    }

    protected void addDebugMessage(String message)
    {
        while (debugMessageQueue.size() >= DEFAULT_DEBUG_QUEUE_SIZE ) {
        	debugMessageQueue.poll();
        }
        
        debugMessageQueue.add(message);
    }

    public String getDebugMessages()
    {
		String debugMessages = "";
		
		for (String debugLine: debugMessageQueue) {
			debugMessages = debugMessages + debugLine + System.getProperty("line.separator");
		}
		return debugMessages;
    }
    
    public String getLogMessages() {
    	return this.logMessages;
    }

    public String getSummaryLogMessages() {
    	return this.summaryLogMessages;
    }
    
    public String getCompoundMessages() {
	   
    	return getSummaryLogMessages() 
    			+  System.getProperty("line.separator")
    			+ System.getProperty("line.separator") 
    			+ System.getProperty("line.separator")
    			+ getLogMessages();
    }
    
}
