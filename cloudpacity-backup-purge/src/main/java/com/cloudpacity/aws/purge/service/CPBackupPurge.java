package com.cloudpacity.aws.purge.service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.model.DeleteSnapshotResult;
import com.amazonaws.services.ec2.model.DeregisterImageResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Snapshot;
import com.amazonaws.services.ec2.model.Tag;
import com.cloudpacity.aws.common.entity.AWSImageEntity;
import com.cloudpacity.aws.common.util.CPLogger;
import com.cloudpacity.aws.purge.CPBackupPurgeEnv;
import com.cloudpacity.aws.purge.pojo.BackupPurgeRequest;
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
public class CPBackupPurge {

    protected AWSCredentials awsCredentials;
    protected CPLogger logger;
    protected CPBackupPurgeEnv backupPurgeEnv;
    public static final String RETURN_CODE_COMPLETE = "Complete";
    public static final String RETURN_CODE_ERROR = "Error";
    
    
    public CPBackupPurge(CPLogger cpLogger, AWSCredentials awsCredentials)
    {
   		this.awsCredentials =  awsCredentials;
		this.logger = cpLogger;
		this.backupPurgeEnv = new CPBackupPurgeEnv();
    }

    public String invoke(BackupPurgeRequest backupPurgeRequest)   
    {
    	try 
    	{
	    	processBackupPurge(backupPurgeRequest);
	    	
	    	logger.logSummary("Backup Purge Completed Successfully");
	    	return RETURN_CODE_COMPLETE;
    	}
	   	catch (Throwable e)
    	{
	   		logger.log("Exception in CPBackupPurge.invoke!");
	   		logger.log(logger.getDebugMessages());
	   		logger.log(e.getMessage());
	   		logger.log(ExceptionUtils.getStackTrace(e));
	   		return RETURN_CODE_ERROR;
	   } 
    }
    
    /**
     * 
     * @param backupPurgeRequest
     * @return
     */
    private void processBackupPurge(BackupPurgeRequest backupPurgeRequest) 
    {
    	
        AWSImageEntity imageEntity = new AWSImageEntity(awsCredentials, CPBackupPurgeEnv.getRegionName(), logger,this.backupPurgeEnv);

    	
    	List<Filter> filterTagValues = populateFilterTags(backupPurgeRequest);
    	Collection<String> owners = new ArrayList<String>();
    	owners.add("self");
    	
    	// Process AMIs
    	List<Image> images = imageEntity.getImagesForFilter(filterTagValues, owners);
    	
		for (Image image : images) 
			processImage(imageEntity, image);
		
    	// Process Snapshots
    	List<Snapshot> snapshots = imageEntity.getSnapshotsForFilter(filterTagValues);
    	
		for (Snapshot snapshot : snapshots) 
			processSnapshot(imageEntity, snapshot);
    	
    }

    /**
     * 
     * @param imageEntity
     * @param image
     */
	private void processImage(AWSImageEntity imageEntity, Image image) 
	{
		ZoneId zoneId = ZoneId.of(CPBackupPurgeEnv.UTC_TIME_ZONE);

		String imageName = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getNameTag(), image.getTags(), "");
		
		ZonedDateTime purgeDateTime = getAMIPurgeDateTime(imageName, image,  zoneId);
		ZonedDateTime currDateTime = ZonedDateTime.now(zoneId);
		
		this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' is being evaluated.  Create date: '" +image.getCreationDate()+
				 "' Purge date: '" + purgeDateTime + "' " + "' Current date: '" + currDateTime + "' " + System.getProperty("line.separator") );

		if (ChronoUnit.DAYS.between(purgeDateTime, currDateTime) >= 0) 
		{
			DeregisterImageResult result = imageEntity.delete(image.getImageId());
			this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' was deleted!  Create date: '" +image.getCreationDate()+
					        "' Purge date: '" + purgeDateTime + "' " +  "' Current datetime: '" + currDateTime +
					        System.getProperty("line.separator") + result.getSdkResponseMetadata());
			this.logger.logSummary("DELETED: Image: '" + imageName + "' id: '" + image.getImageId() + "' !" );
		}
	}
	
	/**
	 * 
	 * @param imageName
	 * @param image
	 * @param zoneId
	 * @return
	 * @throws DateTimeParseException
	 * @throws NumberFormatException
	 */
	private ZonedDateTime getAMIPurgeDateTime(String imageName, Image image,ZoneId zoneId) 
			throws DateTimeParseException, NumberFormatException
	{
		
		ZonedDateTime createdDateTime = null;
		Integer retainDays = null;
		
		// get the creation timestamp from a tag
		String creationTimestampString = AWSImageEntity.getTagValueFromList(CPBackupPurgeEnv.DEFAULT_CREATION_TIMESTAMP_TAG, image.getTags(), "");

		// if a creation timestamp tag is not found, get the creation time from the AMI's creation date
		if (StringUtils.isEmpty(creationTimestampString)) 
		{
			String createdDateString = image.getCreationDate();
			try 
			{
				createdDateTime = ZonedDateTime.parse(createdDateString, DateTimeFormatter.ofPattern(AWSImageEntity.AMI_DATE_FORMAT));
			}
			catch (DateTimeParseException pe) {
				this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' error parsing AMI creation date attribute '" + createdDateString + "'");
				throw pe;
			}
		}
		else 
		{
			try 
			{
				createdDateTime = ZonedDateTime.parse(creationTimestampString, DateTimeFormatter.ofPattern(CPBackupPurgeEnv.DEFAULT_DATE_FORMAT));
			}
			catch (DateTimeParseException pe) 
			{
				this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' error parsing tag creation date '" + creationTimestampString + "'");
				throw pe;
			}
		}		
		
		String retainDaysString = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getBackupRetentionDaysTag(), image.getTags(), "");
		
		// use the tag for retention days if available, otherwise use the the default
		if (!StringUtils.isEmpty(retainDaysString)) 
		{
			try 
			{
				retainDays = new Integer(retainDaysString);
			}
			catch (NumberFormatException nfe) {
				this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' has an invalid tag '" + this.backupPurgeEnv.getBackupRetentionDaysTag() + "'");
				throw nfe;
			}
		}
		else 
		{
			retainDays = CPBackupPurgeEnv.getDefaultRetentionDays();
		}

		return createdDateTime.plusDays(retainDays);

	}
    
	/**
	 * 
	 * @param imageEntity
	 * @param snapshot
	 */
	private void processSnapshot(AWSImageEntity imageEntity, Snapshot snapshot) {
		
		ZonedDateTime purgeDateTime = null;
		ZoneId zoneId = ZoneId.of(CPBackupPurgeEnv.UTC_TIME_ZONE);	
		ZonedDateTime currDateTime = ZonedDateTime.now(zoneId);
		
		String snapshotName = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getNameTag(), snapshot.getTags(), "");
		
		List<Tag> tags = snapshot.getTags();

		try {
			purgeDateTime = getSnapshotPurgeDateTime(snapshotName, snapshot, zoneId);
		}
		catch (DateTimeParseException pe){ return;}
		catch (NumberFormatException pe){ return;}
		
		this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' is being evaluated.  '"  +
		        "' Purge datetime: '" + purgeDateTime + "' " + "' Current datetime: '" + currDateTime + "' " + System.getProperty("line.separator") );


		if (ChronoUnit.DAYS.between(purgeDateTime,currDateTime) >= 0) 
		{
			DeleteSnapshotResult result = imageEntity.deleteSnapshot(snapshot.getSnapshotId());
			this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' was deleted!  '"  +
					        "' Purge datetime: '" + purgeDateTime + "' " +  "' Current datetime: '" + System.getProperty("line.separator") + result.getSdkResponseMetadata());
			this.logger.logSummary("DELETED: Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' !");
		}
	}
	
	/**
	 * 
	 * @param snapshotName
	 * @param snapshot
	 * @param zoneId
	 * @return
	 * @throws DateTimeParseException
	 * @throws NumberFormatException
	 */
	private ZonedDateTime getSnapshotPurgeDateTime(String snapshotName, Snapshot snapshot, ZoneId zoneId) 
				throws DateTimeParseException, NumberFormatException
	{
		
		ZonedDateTime createdDateTime = null;
		Integer retainDays = null;

		String creationTimestampString = AWSImageEntity.getTagValueFromList(CPBackupPurgeEnv.DEFAULT_CREATION_TIMESTAMP_TAG, snapshot.getTags(), "");
		
		if (StringUtils.isEmpty(creationTimestampString)) 
		{
			snapshot.getStartTime();
			createdDateTime = ZonedDateTime.ofInstant(snapshot.getStartTime().toInstant(),zoneId);
		}
		else 
		{
			try 
			{
				createdDateTime = ZonedDateTime.parse(creationTimestampString, DateTimeFormatter.ofPattern(CPBackupPurgeEnv.DEFAULT_DATE_FORMAT));
			}
			catch (DateTimeParseException pe) 
			{
				this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' error parsing creation date '" + creationTimestampString + "'");
				throw pe;
			}
		}
		
		String retainDaysString = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getBackupRetentionDaysTag(), snapshot.getTags(), "");
		
		// use the tag for retention days if available otherwise use the default
		if (!StringUtils.isEmpty(retainDaysString)) {
			try {
				retainDays = new Integer(retainDaysString);
			}
			catch (NumberFormatException nfe) {
				this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' has an invalid tag '" + this.backupPurgeEnv.getBackupRetentionDaysTag() + "'");
				throw nfe;
			}
		}
		else {
			retainDays = CPBackupPurgeEnv.getDefaultRetentionDays();
		}

		return createdDateTime.plusDays(retainDays);
	}
	/**
	 * 
	 * @param backupPurgeRequest
	 * @return
	 */
    private List<Filter>  populateFilterTags(BackupPurgeRequest backupPurgeRequest) 
    {
    	
    	
    	List<Filter> filterArray = new ArrayList<Filter>();
    	// set to filter the AMIs to only list the current AWS account's AMIs
    	Filter ownerFilter = new Filter().withName("owner-id").withValues(backupPurgeRequest.getAwsAccountId());
    	this.logger.log("Account Id: " + backupPurgeRequest.getAwsAccountId());
    	filterArray.add(ownerFilter);
    	
    	if (!StringUtils.isEmpty(CPBackupPurgeEnv.getFilter1TagName()) &&
    		!StringUtils.isEmpty(CPBackupPurgeEnv.getFilter1TagName()))
    	{
	    	Filter tagKeyFilter = new Filter().withName("tag-key").withValues(CPBackupPurgeEnv.getFilter1TagName());
	    	filterArray.add(tagKeyFilter);   
	    	Filter tagvalueFilter = new Filter().withName("tag-value").withValues(CPBackupPurgeEnv.getFilter1TagValue());
	    	filterArray.add(tagvalueFilter);  	    	
	    	this.logger.log("Tag Filter1: " + CPBackupPurgeEnv.getFilter1TagName() + "'  '" + CPBackupPurgeEnv.getFilter1TagValue() + "'");
    	}
    	if (!StringUtils.isEmpty(CPBackupPurgeEnv.getFilter2TagName()) &&
        		!StringUtils.isEmpty(CPBackupPurgeEnv.getFilter2TagName()))
    	{
    	    	Filter tagKeyFilter2 = new Filter().withName("tag-key").withValues(CPBackupPurgeEnv.getFilter2TagName());
    	    	filterArray.add(tagKeyFilter2);   
    	    	Filter tagvalueFilter2 = new Filter().withName("tag-value").withValues(CPBackupPurgeEnv.getFilter2TagValue());
    	    	filterArray.add(tagvalueFilter2);  	    	
    	    	this.logger.log("Tag Filter2: " + CPBackupPurgeEnv.getFilter2TagName() + "'  '" + CPBackupPurgeEnv.getFilter2TagValue() + "'");
        }    	
    	if (!StringUtils.isEmpty(CPBackupPurgeEnv.getFilter3TagName()) &&
        		!StringUtils.isEmpty(CPBackupPurgeEnv.getFilter3TagName()))
    	{
    	    	Filter tagKeyFilter3 = new Filter().withName("tag-key").withValues(CPBackupPurgeEnv.getFilter3TagName());
    	    	filterArray.add(tagKeyFilter3);   
    	    	Filter tagvalueFilter2 = new Filter().withName("tag-value").withValues(CPBackupPurgeEnv.getFilter3TagValue());
    	    	filterArray.add(tagvalueFilter2);  	    	
    	    	this.logger.log("Tag Filter3: " + CPBackupPurgeEnv.getFilter3TagName() + "'  '" + CPBackupPurgeEnv.getFilter3TagValue() + "'");
        }   
    	
    	return filterArray;
    }
}
