package com.cloudpacity.aws.common;


import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.regions.Regions;

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
public class CPCommonEnv {

	
    public static final int DEFAULT_ACTION_PAUSE_SECS = 30;
    public static final int DEFAULT_MAX_RECURSIVE_CALLS = 40;
    public static final int DEFAULT_MAX_RUN_MINUTES = 20;
    public static final int DEFAULT_DB_RETENTION_DAYS = 14;	
    public static final String DEFAULT_TIME_ZONE = "America/Chicago";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss z";
    public static final String DEFAULT_AVAILABLE_DAY_TAG = "AvailableDay";
    public static final String DEFAULT_AVAILABLE_BEGIN_TIME_TAG = "AvailableBeginTime";
    public static final String DEFAULT_AVAILABLE_END_TIME_TAG = "AvailableEndTime";
    public static final String DEFAULT_INSTANCE_DEPENDENCY_TAG = "InstanceDependencies";
    public static final String DEFAULT_CREATION_TIMESTAMP_TAG = "CreationTimestamp";
    public static final String DEFAULT_INSTANCE_TAGS_TO_INCLUDE_TAG = "InstanceTagsToInclude";
    public static final String DEFAULT_INSTANCE_ID_TAG = "InstanceId";
    public static final String DEFAULT_DEVICE_TAG = "Device";
    public static final String DEFAULT_IMAGE_ID_TAG = "ImageId";
    public static final String DEFAULT_BACKUP_STRATEGY_TAG = "BackupStrategy";
    
    public static final String ENV_VAR_OVERRIDE_NAME_TAG = "NameTag";
    public static final String DEFAULT_NAME_TAG = "Name";
    public static final String DEFAULT_RETAIN_DAYS_TAG = "BackupRetentionDays";
    public static final String ENV_VAR_RETAIN_DAYS_TAG = "BackupRetentionDaysTag";
	
    public String getBackupRetentionDaysTag()
    {
        String retainDaysTag = System.getenv(ENV_VAR_RETAIN_DAYS_TAG);
        if(StringUtils.isEmpty(retainDaysTag))
            return DEFAULT_RETAIN_DAYS_TAG;
        else
            return retainDaysTag;
    }
    

    public String getNameTag()
    {
        String NameTag = System.getenv(ENV_VAR_OVERRIDE_NAME_TAG);
        if(StringUtils.isEmpty(NameTag))
            return DEFAULT_NAME_TAG;
        else
            return NameTag;
    }
    
    public static Regions getRegion()
    {
        return Regions.fromName(System.getenv("AWS_DEFAULT_REGION"));
    }

    public static String getRegionName()
    {
        return System.getenv("AWS_DEFAULT_REGION");
    }
    
    public static String getFormattedDate(){

		ZonedDateTime currentDatetime = ZonedDateTime.now(ZoneId.of(DEFAULT_TIME_ZONE));
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT);
		return currentDatetime.format(dateFormatter);
    }
}
