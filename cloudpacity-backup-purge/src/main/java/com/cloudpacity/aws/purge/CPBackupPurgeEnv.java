package com.cloudpacity.aws.purge;

import org.apache.commons.lang3.StringUtils;

import com.cloudpacity.aws.common.CPCommonEnv;
/**
 * 
 * Copyright 2016 Cloudpacity
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
public class CPBackupPurgeEnv extends CPCommonEnv{
	
    public static final String ENV_VAR_TIME_ZONE = "TimeZone";
    public static final String ENV_VAR_FILTER1_TAG_NAME = "Filter1TagName";
    public static final String ENV_VAR_FILTER1_TAG_VALUE = "Filter1TagValue";
    public static final String ENV_VAR_FILTER2_TAG_NAME = "Filter2TagName";
    public static final String ENV_VAR_FILTER2_TAG_VALUE = "Filter2TagValue";
    public static final String ENV_VAR_FILTER3_TAG_NAME = "Filter3TagName";
    public static final String ENV_VAR_FILTER3_TAG_VALUE = "Filter3TagValue"; 

    public static String getDefaultTimeZone()
    {
        String defaultTimeZone = System.getenv(ENV_VAR_TIME_ZONE);
        if(StringUtils.isEmpty(defaultTimeZone))
            return DEFAULT_TIME_ZONE;
        else
            return defaultTimeZone;
    }
    
    public static int getDefaultRetentionDays()
    {
		String defaultRetentionDaysString = System.getenv(DEFAULT_RETAIN_DAYS_TAG);
		if (StringUtils.isEmpty(defaultRetentionDaysString)) {
			return DEFAULT_RETENTION_DAYS;
		}
		try {
			return new Integer(defaultRetentionDaysString);
		}
		catch (NumberFormatException nfe) {
			return DEFAULT_RETENTION_DAYS;
		}
    }
    
    public static String getFilter1TagName() {
		return System.getenv(ENV_VAR_FILTER1_TAG_NAME);
    }
    
    public static String getFilter1TagValue() {
		return System.getenv(ENV_VAR_FILTER1_TAG_VALUE);
    }
    
    public static String getFilter2TagName() {
		return System.getenv(ENV_VAR_FILTER2_TAG_NAME);
    }
    
    public static String getFilter2TagValue() {
		return System.getenv(ENV_VAR_FILTER2_TAG_VALUE);
    }
    
    public static String getFilter3TagName() {
		return System.getenv(ENV_VAR_FILTER3_TAG_NAME);
    }
    
    public static String getFilter3TagValue() {
		return System.getenv(ENV_VAR_FILTER3_TAG_VALUE);
    }
    
   
    
    
}
