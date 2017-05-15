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
    public static final String UTC_TIME_ZONE = "UTC"; 
    
    
    public CPBackupPurge(CPLogger cpLogger, AWSCredentials awsCredentials)
    {
   		this.awsCredentials =  awsCredentials;
		this.logger = cpLogger;
		this.backupPurgeEnv = new CPBackupPurgeEnv();
    }

    public String invoke(BackupPurgeRequest backupPurgeRequest)
    {
    	String returnMessage = "OK";
    	
    	processBackupPurge(backupPurgeRequest);
    	
    	logger.logSummary("Backup Purge Completed Successfully");
    	return returnMessage;
    }
    
    /**
     * 
     * @param backupPurgeRequest
     * @return
     */
    private String processBackupPurge(BackupPurgeRequest backupPurgeRequest) {
    	
        AWSImageEntity imageEntity = new AWSImageEntity(awsCredentials, CPBackupPurgeEnv.getRegionName(), logger,this.backupPurgeEnv);

    	
    	List<Filter> filterTagValues = populateFilterTags(backupPurgeRequest);
    	Collection<String> owners = new ArrayList<String>();
    	owners.add("self");
    	
    	List<Image> images = imageEntity.getImagesForFilter(filterTagValues, owners);
    	
		for (Image image : images) {
			processImage(imageEntity, image);
		}
		
    	List<Snapshot> snapshots = imageEntity.getSnapshotsForFilter(filterTagValues);
    	
		for (Snapshot snapshot : snapshots) {
			processSnapshot(imageEntity, snapshot);
		}
    	
    	return "";
    }

    /**
     * 
     * @param imageEntity
     * @param image
     */
	private void processImage(AWSImageEntity imageEntity, Image image) {
		
		ZonedDateTime createdDateTime = null;
		long retainDays = 365; // 1 year
		String creationTimestampString = AWSImageEntity.getTagValueFromList(CPBackupPurgeEnv.DEFAULT_CREATION_TIMESTAMP_TAG, image.getTags(), "");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CPBackupPurgeEnv.DEFAULT_DATE_FORMAT); 
		String imageName = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getNameTag(), image.getTags(), "");
		
		try {
			createdDateTime = ZonedDateTime.parse(creationTimestampString, formatter);
		}
		catch (DateTimeParseException pe) {
			this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' error parsing creation date '" + creationTimestampString + "'");
			return;
		}
		
		String retainDaysString = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getBackupRetentionDaysTag(), image.getTags(), "");
		
		try {
			retainDays = new Long(retainDaysString);
		}
		catch (NumberFormatException nfe) {
			this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' has an invalid tag '" + this.backupPurgeEnv.getBackupRetentionDaysTag() + "'");
			return;
		}
		
		ZoneId zoneId = ZoneId.of(UTC_TIME_ZONE);
		ZonedDateTime currDateTime = ZonedDateTime.now(zoneId);
		
		ZonedDateTime purgeDateTime = createdDateTime.plusDays(retainDays);
		
		if (ChronoUnit.DAYS.between(purgeDateTime, currDateTime) > 0) {
			DeregisterImageResult result = imageEntity.delete(image.getImageId());
			this.logger.log("Image: '" + imageName + "' id: '" + image.getImageId() + "' was deleted!  Create date: '" +image.getCreationDate()+
					        "' Retain days: '" + retainDays + "' " + System.getProperty("line.separator") + result.getSdkResponseMetadata());
			this.logger.logSummary("Image: '" + imageName + "' id: '" + image.getImageId() + "' was deleted!  Create date: '" +image.getCreationDate()+
					                "' Retained for: '" + retainDays + "' days" );
		}
	}
    
	private void processSnapshot(AWSImageEntity imageEntity, Snapshot snapshot) {
		
		Date parsedDate = null;
		ZonedDateTime createdDateTime = null;
		long retainDays = 365; // 1 year
		String snapshotName = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getNameTag(), snapshot.getTags(), "");
		
		List<Tag> tags = snapshot.getTags();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CPBackupPurgeEnv.DEFAULT_DATE_FORMAT); 

		String creationTimestampString = AWSImageEntity.getTagValueFromList(CPBackupPurgeEnv.DEFAULT_CREATION_TIMESTAMP_TAG, snapshot.getTags(), "");
		
		try {
			createdDateTime = ZonedDateTime.parse(creationTimestampString, formatter);
		}
		catch (DateTimeParseException pe) {
			this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' error parsing creation date '" + creationTimestampString + "'");
			return;
		}
		
		String retainDaysString = AWSImageEntity.getTagValueFromList(this.backupPurgeEnv.getBackupRetentionDaysTag(), snapshot.getTags(), "");
		
		try {
			retainDays = new Long(retainDaysString);
		}
		catch (NumberFormatException nfe) {
			this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' has an invalid tag '" + this.backupPurgeEnv.getBackupRetentionDaysTag() + "'");
			return;
		}
		
		ZoneId zoneId = ZoneId.of(UTC_TIME_ZONE);
		ZonedDateTime currDateTime = ZonedDateTime.now(zoneId);
		
		ZonedDateTime purgeDateTime = createdDateTime.plusDays(retainDays);
		
		if (ChronoUnit.DAYS.between(purgeDateTime, currDateTime) > 0) {
			DeleteSnapshotResult result = imageEntity.deleteSnapshot(snapshot.getSnapshotId());
			this.logger.log("Snapshot: '" + snapshotName + "' id: '" + snapshot.getSnapshotId() + "' was deleted!  Create date: '" + creationTimestampString +
					        "' Retain days: '" + retainDays + "' " + System.getProperty("line.separator") + result.getSdkResponseMetadata());
		}
	}
	
	/**
	 * 
	 * @param backupPurgeRequest
	 * @return
	 */
    private List<Filter>  populateFilterTags(BackupPurgeRequest backupPurgeRequest) {
    	
    	
    	List<Filter> filterArray = new ArrayList<Filter>();
    	// set to filter the AMIs to only list the current AWS account's AMIs
    	Filter filter = new Filter().withName("owner-id").withValues(backupPurgeRequest.getAwsAccountId());
    	this.logger.log("Account Id: " + backupPurgeRequest.getAwsAccountId());
    	filterArray.add(filter);

    	return filterArray;
    }
}
