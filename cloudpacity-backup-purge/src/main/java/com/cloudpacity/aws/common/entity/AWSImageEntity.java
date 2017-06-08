package com.cloudpacity.aws.common.entity;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.CreateSnapshotRequest;
import com.amazonaws.services.ec2.model.CreateSnapshotResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteSnapshotRequest;
import com.amazonaws.services.ec2.model.DeleteSnapshotResult;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DeregisterImageResult;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMapping;
import com.amazonaws.services.ec2.model.Snapshot;
import com.amazonaws.services.ec2.model.Tag;
import com.cloudpacity.aws.common.CPCommonEnv;
import com.cloudpacity.aws.common.error.CPRuntimeException;
import com.cloudpacity.aws.common.error.MultipleObjectsReturnedException;
import com.cloudpacity.aws.common.util.CPLogger;

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
public class AWSImageEntity extends AWSObjectEntity
{
    public static final int DEFAULT_IMAGE_WAIT_SECS = 60;
    public static final String AMI_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

    private AWSCredentials awsCredentials;
    private CPCommonEnv env;
    
    protected AmazonEC2 ec2Client;
    
    
	public AWSImageEntity (AWSCredentials awsCredentials, String regionName, CPLogger logger, CPCommonEnv env ) {
		
		super( logger,env);
		Validate.notNull(awsCredentials, "The AWS credentials supplied were null!");
		Validate.notEmpty(regionName, "The AWS region name provided is empty!");
		Validate.notNull(env, "The environment parms are null!");	
		Validate.notNull(logger, "The logger is null!");	
		
		this.env = env;
		this.logger =  logger;
		this.awsCredentials = awsCredentials;

        this.ec2Client = AmazonEC2ClientBuilder.standard()
				.withRegion(regionName)
				.build();
	}
	
	public List<String> createBackup(List<Instance> instanceList, List<String> tagsToInclude, String backupStrategyTag
			, String timestampTag,  String deviceTag, String instanceIdTag, String imageIdTag) throws InterruptedException {
		
		Validate.notNull(instanceList, "The instance list provided is null!");
		
		 List<String> imageIdList = new ArrayList<String>();
		 Map<String, Instance> imageMap = new HashMap<String, Instance>();
		 List<String> snapshotIdList = new ArrayList<String>();
		 Map<String, Instance> snapshotInstanceMap = new HashMap<String, Instance>();
		 Map<String, InstanceBlockDeviceMapping> snapshotDeviceMap = new HashMap<String, InstanceBlockDeviceMapping>();
		
		ZonedDateTime currentDatetime = ZonedDateTime.now(ZoneId.of(CPCommonEnv.DEFAULT_TIME_ZONE));
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(CPCommonEnv.DEFAULT_DATE_FORMAT);
		String formattedCurrentDatetime = currentDatetime.format(dateFormatter);
		// ami names can't have ":"
		String amiFormattedCurrentDatetime = formattedCurrentDatetime.replace(":", ".");
		
        AWSInstanceEntity instanceEntity = new AWSInstanceEntity(this.awsCredentials, CPCommonEnv.getRegionName(), this.logger, this.env);
		
		for (Instance instance: instanceList ){
			
			if (instance != null && StringUtils.isNotEmpty(instance.getInstanceId())) {
				
				String instanceName = AWSInstanceEntity.getTagValueFromList(this.env.getNameTag(), instance.getTags(),"");
				
				if (instanceEntity.amiBackup(instance, backupStrategyTag)) {
					CreateImageResult createImageResult = createImage(instanceList, tagsToInclude, instance
																	  , amiFormattedCurrentDatetime, formattedCurrentDatetime);
					
					imageIdList.add(createImageResult.getImageId());
					imageMap.put(createImageResult.getImageId(), instance);

					logger.logSummary(instanceName + " backed up via AMI '" + createImageResult.getImageId() + "' ");
				}
				else {
					for (InstanceBlockDeviceMapping blockDevice: instance.getBlockDeviceMappings()){
						
						CreateSnapshotResult createSnapshotResult = createSnapshot(instanceList, tagsToInclude, instance
								, blockDevice, formattedCurrentDatetime);
						
						if (createSnapshotResult != null) {
							snapshotIdList.add(createSnapshotResult.getSnapshot().getSnapshotId());
							snapshotInstanceMap.put(createSnapshotResult.getSnapshot().getSnapshotId(), instance);
							snapshotDeviceMap.put(createSnapshotResult.getSnapshot().getSnapshotId(), blockDevice);
							logger.logSummary(instanceName + " device: " + blockDevice.getDeviceName() + " backed up via snapshot '" + createSnapshotResult.getSnapshot().getSnapshotId() + "' ");
						}
					}
				}
				
			}
		}
		this.logger.log("Backups created, tagging started");
		Thread.sleep(DEFAULT_IMAGE_WAIT_SECS*1000); //sleep for 20 secs
		
		tagImages(imageIdList,  imageMap,  formattedCurrentDatetime, tagsToInclude, timestampTag, deviceTag, instanceIdTag, imageIdTag);
		
		tagSnapshots(snapshotIdList, snapshotDeviceMap,  snapshotInstanceMap, formattedCurrentDatetime,tagsToInclude, timestampTag, deviceTag, instanceIdTag, imageIdTag);
		
		return imageIdList;
	}


	public CreateImageResult createImage(List<Instance> instanceList, List<String> tagsToInclude, Instance instance, String amiFormattedCurrentDatetime
			, String formattedCurrentDatetime) {
		
				List<Tag> instanceTagList = instance.getTags();
		
				CreateImageRequest createImageRequest = new CreateImageRequest();
				createImageRequest.setInstanceId(instance.getInstanceId());
				createImageRequest.setName(getTagValue(instanceTagList,this.env.getNameTag()) + "_" + amiFormattedCurrentDatetime);
				createImageRequest.setDescription(getTagValue(instanceTagList,this.env.getNameTag()) + "_" + formattedCurrentDatetime);
				
				return ec2Client.createImage(createImageRequest);
				
	}
				
	public void tagImages(List<String> imageIdList, Map<String, Instance> imageMap, String formattedCurrentDatetime,List<String> tagsToInclude
			, String timestampTag,  String deviceTag, String instanceIdTag, String imageIdTag) {
		

		for (String imageId: imageIdList) {
			
			if (isInFailedState(imageId)) {
				logger.log("ERROR: Image: " + imageId + " is in a failed state!");
			}
			else {
				Instance instance = imageMap.get(imageId);
				
				if (instance == null) {
					throw new CPRuntimeException("instance null when tagging the image! Image Id: " + imageId + " tags: " + tagsToInclude);
				}
				
				tagImageFromInstance(instance, imageId, formattedCurrentDatetime,tagsToInclude,timestampTag, instanceIdTag);
				tagAMISnapshotsFromInstance(instance, imageId, formattedCurrentDatetime,tagsToInclude, instance.getInstanceId()
						, timestampTag,  deviceTag, instanceIdTag, imageIdTag);
			}
		}
		
		this.logger.log("Images tagging completed");

	}
	

	public CreateSnapshotResult createSnapshot(List<Instance> instanceList, List<String> tagsToInclude, Instance instance
			, InstanceBlockDeviceMapping blockDevice, String formattedCurrentDatetime) throws InterruptedException {
				
	
					String deviceName = blockDevice.getDeviceName();
					String volumeId = blockDevice.getEbs().getVolumeId();
					List<Tag> instanceTagList = instance.getTags();
					
					CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest();
					
					if (StringUtils.isNotEmpty(volumeId)) {

						createSnapshotRequest.setVolumeId(volumeId);
						createSnapshotRequest.setDescription(getTagValue(instanceTagList,this.env.getNameTag()) + " " + deviceName + " " + formattedCurrentDatetime);
						
						return ec2Client.createSnapshot(createSnapshotRequest);
					}
					
					return null;

	}
	
	private void tagSnapshots( List<String> snapshotIdList, Map<String, InstanceBlockDeviceMapping> snapshotDeviceMap, Map<String, Instance> snapshotInstanceMap
			, String formattedCurrentDatetime,List<String> tagsToInclude, String timestampTag,  String deviceTag, String instanceIdTag, String imageIdTag) {
		

		for (String snapshotId: snapshotIdList) {
		
			if (snpashotInFailedState(snapshotId)) {
				logger.log("ERROR: Snapshot: " + snapshotId + " is in a failed state!");
			}
			else {
				InstanceBlockDeviceMapping blockDevice =  snapshotDeviceMap.get(snapshotId);
				Instance instance =  snapshotInstanceMap.get(snapshotId);
				
				if (instance == null) {
					throw new CPRuntimeException("instance null when tagging the snapshot! Snapshot Id: " + snapshotId + " tags: " + tagsToInclude);
				}
				if (blockDevice == null) {
					throw new CPRuntimeException("instance EBS was null when tagging the snapshot! Snapshot Id: " + snapshotId + " tags: " + tagsToInclude);
				}
				
				Snapshot snapshot = getSnapshotForId(snapshotId);
				
				if (snapshot == null) {
					throw new CPRuntimeException("Snapshot: '" + snapshotId + "' was not found when tagging the snapshot! Snapshot Id: " + snapshotId + " tags: " + tagsToInclude);
				}
				
				Map<String,Tag> instanceTagMap = getTagMap( instance.getTags());
				
				tagSnapshot("", formattedCurrentDatetime, tagsToInclude, instanceTagMap, blockDevice.getDeviceName(), snapshot, instance.getInstanceId()
						, timestampTag,  deviceTag, instanceIdTag, imageIdTag);
			}
		}
	}

	public void tagImageFromInstance(Instance instance, String imageId, String formattedCreationDate
			, List<String> tagsToInclude, String timestampTag, String instanceIdTag) {
		
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		List<Tag> imageTags = new ArrayList<Tag>();
		
		Map<String,Tag> instanceTagMap = getTagMap( instance.getTags());
		
		Set<String> tagNameSet = instanceTagMap.keySet();
		
		for (String tagName: tagNameSet) {
		
			if (this.env.getNameTag().equalsIgnoreCase(tagName)) {
				Tag nameTag = instanceTagMap.get(tagName);
				imageTags.add(new Tag(this.env.getNameTag(), nameTag.getValue() + "_" + formattedCreationDate));
			}
			else {
				Tag tagToAdd = instanceTagMap.get(tagName);
				logger.logDebug("tagname: " + tagName);
				if (isValidTag(tagToAdd,tagsToInclude)) {
					imageTags.add(new Tag(tagName, tagToAdd.getValue()));
					logger.logDebug("added tag: " + tagName);
				}
			}
		}
		
		imageTags.add(new Tag(timestampTag,formattedCreationDate));	
		imageTags.add(new Tag(instanceIdTag,instance.getInstanceId()));		
		
		createTagsRequest.setTags(imageTags);
		createTagsRequest.setResources(Collections.singletonList(imageId));
		
		ec2Client.createTags(createTagsRequest);
		logger.logDebug("tagged image id: " + imageId);
	}


	
	/**
	 * 
	 * @param instance
	 * @param imageId
	 * @param formattedCreationDate
	 * @param tagsToInclude
	 */
	public void tagAMISnapshotsFromInstance(Instance instance, String imageId, String formattedCreationDate
			,List<String>tagsToInclude, String instanceId, String timestampTag,  String deviceTag, String instanceIdTag, String imageIdTag) {
		
		List<Image> imageList = getImagesForIds(Collections.singletonList(imageId));
		Map<String,Tag> instanceTagMap = getTagMap( instance.getTags());
		
		for (Image image: imageList) {
			
			for (BlockDeviceMapping blockDevice: image.getBlockDeviceMappings()){
				
				String deviceName = blockDevice.getDeviceName();
				String snapshotId = blockDevice.getEbs().getSnapshotId();
				
				if (StringUtils.isNotEmpty(snapshotId)) {
					DescribeSnapshotsRequest describeSnapshotsRequest = new DescribeSnapshotsRequest();
					
					describeSnapshotsRequest.setSnapshotIds(Collections.singletonList(snapshotId));
					
					DescribeSnapshotsResult snapResult = ec2Client.describeSnapshots(describeSnapshotsRequest);
					
					for (Snapshot snapshot: snapResult.getSnapshots()) {
						
						tagSnapshot(imageId, formattedCreationDate, tagsToInclude, instanceTagMap, deviceName, snapshot, instanceId
								, timestampTag,  deviceTag, instanceIdTag, imageIdTag);					
					}
				}
				else {
					logger.log("Error, snapshot id is empty for image: "+ imageId);
				}
			}
		}
	}

	private void tagSnapshot(String imageId, String formattedCreationDate, List<String> tagsToInclude,
			Map<String, Tag> instanceTagMap, String deviceName, Snapshot snapshot, String instanceId
			, String timestampTag,  String deviceTag, String instanceIdTag, String imageIdTag) {
		
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		List<Tag> snapshotTags = new ArrayList<Tag>();

		Set<String> tagNameSet = instanceTagMap.keySet();
		
		for (String tagName: tagNameSet) {
		
			if (this.env.getNameTag().equalsIgnoreCase(tagName)) {
				Tag nameTag = instanceTagMap.get(tagName);
				snapshotTags.add(new Tag(this.env.getNameTag(), nameTag.getValue() + " " + deviceName + " " + formattedCreationDate));
			}
			else {
				Tag tagToAdd = instanceTagMap.get(tagName);
				if (isValidTag(tagToAdd,tagsToInclude)) {
					snapshotTags.add( tagToAdd);
				}
			}
		}
		
		snapshotTags.add(new Tag(timestampTag,formattedCreationDate));	
		snapshotTags.add(new Tag(deviceTag,deviceName));		
		snapshotTags.add(new Tag(instanceIdTag,instanceId));
		snapshotTags.add(new Tag(imageIdTag, imageId));	
			
		createTagsRequest.setTags(snapshotTags);
		createTagsRequest.setResources(Collections.singletonList(snapshot.getSnapshotId()));
		
		ec2Client.createTags(createTagsRequest);
		logger.log("tagged snapshot id: " + snapshot.getSnapshotId());
	}

    public boolean isInFailedState(String imageId)
    {
        Image image = getImageForId(imageId);
        return (image != null) & "failed".equalsIgnoreCase(image.getState());
    }
    
    public boolean snpashotInFailedState(String snapshotId)
    {
        Snapshot image = getSnapshotForId(snapshotId);
        return (image != null) & "failed".equalsIgnoreCase(image.getState());
    }

	public void waitForState(List<String> imageIds, String desiredState1, String desiredState2, int maxWaitSecs)
	 		throws InterruptedException {
		
		
		Validate.notNull(imageIds, "Image id list is null!");
		Validate.notEmpty(desiredState1, "The desired state1 is empty!");
		Validate.notEmpty(desiredState2, "The desired state2 is empty!");
		
		String collectiveImageState = "begin";  // a non existent test state to get the ball rolling
		int totalWaitTime = 0;
		
		if (imageIds.size() == 0) {
			return;
		}
		
		while (!collectiveImageState.equalsIgnoreCase(desiredState1) &&
			   !collectiveImageState.equalsIgnoreCase(desiredState2)) {
			
			List<Image> currentImageList = this.getImagesForIds(imageIds);
			collectiveImageState = desiredState1;
			
			for (Image image: currentImageList) {
				
				if (image != null) {
					
					if (image.getState() != null) {
						
						if (!desiredState1.equalsIgnoreCase(image.getState()) &&
							!desiredState2.equalsIgnoreCase(image.getState())) {
							collectiveImageState = image.getState();
						}
						else {
							
						}
					}
				}
				
			}
			this.logger.logDebug("Image wait time: " + totalWaitTime + " secs for state: " + desiredState1 + " or " + desiredState2);
			Thread.sleep(5000); //sleep for 5 secs	
			totalWaitTime = totalWaitTime + 10;
			
			if (totalWaitTime > maxWaitSecs) {	
				this.logger.logDebug("waitForInstanceState max wait time of " + maxWaitSecs + " exceeded!");
				throw new CPRuntimeException("waitForInstanceState max wait time of " + maxWaitSecs + " secs exceeded!");
			}
		}
	}

	public List<Image> getImagesForIds(List<String> imageIds) {
		
		Validate.notNull(imageIds,"Image id list is null!");
		
		DescribeImagesRequest request = new DescribeImagesRequest();
		request.setImageIds(imageIds);
		
		DescribeImagesResult result = ec2Client.describeImages(request);

		return result.getImages();
	}

	public Image getImageForId(String imageId) throws MultipleObjectsReturnedException {
		
		Validate.notEmpty(imageId,"Image id  is emtpy!");
		
		List<String> imageIdList = new ArrayList<String>();
		imageIdList.add(imageId);
		List<Image> imageList = getImagesForIds(imageIdList);
		
		if (imageList.size()> 1) {
			throw new MultipleObjectsReturnedException("Multiple images returned for id: " + imageId);
		}

		return imageList.get(0);
	}
	
    public List<Image> getImagesForFilter(List<Filter> filters,Collection<String> owners)
    {
        Validate.notNull(filters, "The filter given is null!", new Object[0]);

        DescribeImagesRequest request = new DescribeImagesRequest();
        request.setFilters(filters);
        if (owners != null && owners.size() >0) {
        	request.setOwners(owners);
        }

        DescribeImagesResult result =  ec2Client.describeImages(request);
        return result.getImages();
    }

	public DeregisterImageResult delete(String imageId) {
		
        Validate.notEmpty(imageId, "The image id was empty!");	
        
        DeregisterImageRequest deregisterImageRequest = new DeregisterImageRequest();
        deregisterImageRequest.setImageId(imageId);
		
        return ec2Client.deregisterImage(deregisterImageRequest);
	}
	
	public DeleteSnapshotResult deleteSnapshot(String snapshotId) {
		
        Validate.notEmpty(snapshotId, "The snapshot id was empty!");	
        
        DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest();
        deleteSnapshotRequest.setSnapshotId(snapshotId);
		
        return ec2Client.deleteSnapshot(deleteSnapshotRequest);
	}
	
	public List<Snapshot> getSnapshotsForIds(List<String> snapshotIds) {
		
		Validate.notNull(snapshotIds,"Snapshot id list is null!");
		
		DescribeSnapshotsRequest request = new DescribeSnapshotsRequest();
		request.setSnapshotIds(snapshotIds);
		
		DescribeSnapshotsResult result = ec2Client.describeSnapshots(request);

		return result.getSnapshots();
	}

	
	public Snapshot getSnapshotForId(String snapshotId) throws MultipleObjectsReturnedException {
		
		Validate.notEmpty(snapshotId,"Snapshot id  is emtpy!");
		
		List<String> snpashotIdList = new ArrayList<String>();
		snpashotIdList.add(snapshotId);
		List<Snapshot> snapshotList = getSnapshotsForIds(snpashotIdList);
		
		if (snapshotList.size()> 1) {
			throw new MultipleObjectsReturnedException("Multiple snapshots returned for id: " + snapshotId);
		}

		return snapshotList.get(0);
	}

	
    public List<Snapshot> getSnapshotsForFilter(List<Filter> filters)
    {
        Validate.notNull(filters, "The filter given is null!", new Object[0]);

        DescribeSnapshotsRequest request = new DescribeSnapshotsRequest();
        request.setFilters(filters);

        DescribeSnapshotsResult result =  ec2Client.describeSnapshots(request);
        return result.getSnapshots();
    }
    
	private String getTagValue(List<Tag>instanceTagList, String tagName) {
		
		String tagValue = "";
		
		for (Tag instanceTag: instanceTagList){
			
			if (tagName.equalsIgnoreCase(instanceTag.getKey())) {
				
				if (instanceTag.getValue().isEmpty()) {
					return tagValue;
				}
				else {
					return instanceTag.getValue();
				}
			}
			
			
		}
		
		return tagValue;
	}

	private Map<String,Tag> getTagMap(List<Tag> tagList){
		
		Map<String,Tag> tagMap = new HashMap<String,Tag>();
		if (tagList == null) { return tagMap;}
		
		for (Tag tag: tagList){
			if (StringUtils.isNotEmpty(tag.getKey() )) {
				tagMap.put(tag.getKey() , tag );
			}
		}
	
		return tagMap;
	}

	private boolean isValidTag(Tag tag,List<String> tagsToInclude){
		
		if(tag == null) {
			return false;
		}
		if(StringUtils.isEmpty(tag.getKey())) {
			return false;
		}
		if (tagsToInclude.contains(tag.getKey())){
			logger.logDebug("valid:" + tag.getKey());
			return true;
		}
		else {
			return false;
		}

	}


}
