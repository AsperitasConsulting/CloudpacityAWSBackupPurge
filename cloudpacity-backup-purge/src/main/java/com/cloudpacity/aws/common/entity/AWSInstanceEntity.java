package com.cloudpacity.aws.common.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
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
public class AWSInstanceEntity extends AWSObjectEntity
{
    public static final String RUNNING_STATE = "running";
    public static final String STOPPED_STATE = "stopped";
    public static final String STOPPING_STATE = "stopping";
    public static final String PENDING_STATE = "pending";
    public static final String SHUTTING_DOWN_STATE = "shutting-down";
    public static final String TERMINATED_STATE = "terminated";
    public static final String BACKUP_STRATEGY_AMI_CONST = "AMI";
    public static final String BACKUP_STRATEGY_SNAPSHOT_RUNNING_CONST = "SnapshotRunning";
    public static final String BACKUP_STRATEGY_SNAPSHOT_STOPPED_CONST = "SnapshotStopped";
    
    protected AmazonEC2 ec2Client;
    
    public AWSInstanceEntity(AWSCredentials awsCredentials, String regionName, CPLogger logger,CPCommonEnv env)
    {
        super(logger,env);
        ec2Client = null;
        Validate.notNull(awsCredentials, "The AWS credentials supplied were null!", new Object[0]);
        Validate.notEmpty(regionName, "The AWS region name provided is empty!", new Object[0]);
        this.logger = logger;
        ec2Client = (AmazonEC2)((AmazonEC2ClientBuilder)AmazonEC2ClientBuilder.standard().withRegion(regionName)).build();
    }

    public StartInstancesResult startInstancesByTag(String tagName, String tagValue)
    {
        Validate.notEmpty(tagName, "The tag name given is empty or null!", new Object[0]);
        Validate.notEmpty(tagValue, "The tag value given is empty or null!", new Object[0]);
        List<Instance> instances = getInstancesForTag(tagName, tagValue);
        List<String> instanceIds = new ArrayList<String>();
        Instance instance;
        for(Iterator<Instance> iterator = instances.iterator(); iterator.hasNext(); instanceIds.add(instance.getInstanceId()))
            instance = (Instance)iterator.next();

        return startInstancesById(instanceIds);
    }

    public StartInstancesResult startInstances(Collection<Instance> instances)
    {
        Validate.notEmpty(instances, "The instances given are empty or null!", new Object[0]);
        List<String> ids = new ArrayList<String>();
        for(Iterator<Instance> iterator = instances.iterator(); iterator.hasNext();)
        {
            Instance instance = (Instance)iterator.next();
            if(instance != null && instance.getInstanceId() != null)
                ids.add(instance.getInstanceId());
        }

        return startInstancesById(ids);
    }

    public StopInstancesResult stopInstancesByTag(String tagName, String tagValue)
    {
        Validate.notEmpty(tagName, "The tag name given is empty or null!", new Object[0]);
        Validate.notEmpty(tagValue, "The tag value given is empty or null!", new Object[0]);
        List<Instance> instances = getInstancesForTag(tagName, tagValue);
        List<String> instanceIds = new ArrayList<String>();
        Instance instance;
        for(Iterator<Instance> iterator = instances.iterator(); iterator.hasNext(); instanceIds.add(instance.getInstanceId()))
            instance = (Instance)iterator.next();

        return stopInstancesById(instanceIds);
    }

    public StopInstancesResult stopInstances(Collection<Instance> instances)
    {
        Validate.notEmpty(instances, "The instances given are empty or null!", new Object[0]);
        List<String> instanceIds = new ArrayList<String>();
        for(Iterator<Instance> iterator = instances.iterator(); iterator.hasNext();)
        {
            Instance instance = (Instance)iterator.next();
            if(instance != null && instance.getInstanceId() != null)
                instanceIds.add(instance.getInstanceId());
        }

        return stopInstancesById(instanceIds);
    }

    public StopInstancesResult stopInstancesById(Collection<String> instanceIds)
    {
        Validate.notEmpty(instanceIds, "The instance ids given are empty or null!", new Object[0]);
        StopInstancesRequest instanceReq = new StopInstancesRequest();
        instanceReq.setInstanceIds(instanceIds);
        return ec2Client.stopInstances(instanceReq);
    }

    public StopInstancesResult stopInstance(String instanceId)
    {
        Validate.notEmpty(instanceId, "The instance id given is empty or null!", new Object[0]);
        ArrayList<String> instanceIds = new ArrayList<String>();
        instanceIds.add(instanceId);
        return stopInstancesById(instanceIds);
    }

    public StartInstancesResult startInstancesById(Collection<String> instanceIds)
    {
        Validate.notEmpty(instanceIds, "The Instance Ids given are empty or null!", new Object[0]);
        StartInstancesRequest instanceReq = new StartInstancesRequest();
        instanceReq.setInstanceIds(instanceIds);
        return ec2Client.startInstances(instanceReq);
    }

//    public void backupByTag(String tagName, String tagValue)
//    {
//        List<Instance> instancesToBackup = getInstancesForTag(tagName, tagValue);
//        stopInstances(instancesToBackup);
//        createAMIs(instancesToBackup);
//        startInstances(instancesToBackup);
//    }
//
//    private void createAMIs(List<Instance> instances)
//    {
//        CreateImageRequest createImageReq = new CreateImageRequest();
//        createImageReq.setName("");
//        ec2Client.createImage(createImageReq);
//    }

    public void deleteTags(String instanceId, String tagNames[])
    {
        Validate.notEmpty(instanceId, "The instance id given is empty!", new Object[0]);
        Validate.notEmpty(tagNames, "The tag names given are empty", new Object[0]);
        DeleteTagsRequest deleteTagsRequest = new DeleteTagsRequest();
        Collection<String> instanceIdArray = Arrays.asList(new String[] {
            instanceId
        });
        deleteTagsRequest.setResources(instanceIdArray);
        Collection<Tag> tags = new ArrayList<Tag>();
        String as[];
        int j = (as = tagNames).length;
        for(int i = 0; i < j; i++)
        {
            String tagName = as[i];
            if(!StringUtils.isEmpty(tagName))
                tags.add(new Tag(tagName, null));
        }

        deleteTagsRequest.setTags(tags);
        ec2Client.deleteTags(deleteTagsRequest);
    }

    public static List<String> getInstanceDependencyIdArray(List<Tag> tags, String instanceDependencyTag)
    {
        List<String> dependentIdArray = new ArrayList<String>();
        if(!StringUtils.isEmpty(instanceDependencyTag))
        {
            String instanceDependencies = getTagValueFromList(instanceDependencyTag, tags, "");
            String idArray[] = StringUtils.split(instanceDependencies, ",");
            String as[];
            int j = (as = idArray).length;
            for(int i = 0; i < j; i++)
            {
                String instanceId = as[i];
                if(StringUtils.isNotEmpty(instanceId))
                    dependentIdArray.add(instanceId.trim());
            }

        }
        return dependentIdArray;
    }

    public StartInstancesResult startInstance(String instanceId)
    {
        Validate.notEmpty(instanceId, "The Instance Id given is empty or null!", new Object[0]);
        ArrayList<String> instanceIds = new ArrayList<String>();
        instanceIds.add(instanceId);
        return startInstancesById(instanceIds);
    }

    public ArrayList<Instance> getInstancesForTag(String tagName, String tagValue)
    {
        Validate.notEmpty(tagName, "The tag name given is empty or null!", new Object[0]);
        Validate.notEmpty(tagValue, "The tag value given is empty or null!", new Object[0]);
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        Filter queryFilter = new Filter();
        queryFilter.setName((new StringBuilder("tag:")).append(tagName).toString());
        Collection<String> tagValues = new ArrayList<String>();
        tagValues.add(tagValue);
        queryFilter.setValues(tagValues);
        request.withFilters(new Filter[] {
            queryFilter
        });
        DescribeInstancesResult result = ec2Client.describeInstances(request);
        return getInstancesFromResult(result);
    }

    public ArrayList<Instance> getInstancesForTags(List<Filter> filterTagValues)
    {
        Validate.notNull(filterTagValues, "The filter given is null!", new Object[0]);
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        request.setFilters(filterTagValues);
        DescribeInstancesResult result = ec2Client.describeInstances(request);
        return getInstancesFromResult(result);
    }

    public List<Instance> getInstancesForIds(List<String> instanceIds)
    {
        Validate.notNull(instanceIds, "Instance id list is null!", new Object[0]);
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        request.setInstanceIds(instanceIds);
        DescribeInstancesResult result = ec2Client.describeInstances(request);
        return getInstancesFromResult(result);
    }

    public Instance getInstanceForId(String instanceId)
    {
        Validate.notEmpty(instanceId, "Instance id is empty!", new Object[0]);
        List<String> instanceIds = new ArrayList<String>();
        instanceIds.add(instanceId);
        List<Instance> instanceList = getInstancesForIds(instanceIds);
        if(instanceList.size() > 1)
            throw new MultipleObjectsReturnedException((new StringBuilder("Multiple instances returned for id: ")).append(instanceId).toString());
        else
            return (Instance)instanceList.get(0);
    }

    public ArrayList<Instance> getAllInstances()
    {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult result = ec2Client.describeInstances(request);
        return getInstancesFromResult(result);
    }

    public void waitForState(List<String> instanceIds, String desiredState, int maxWaitSecs)
        throws InterruptedException
    {
        Validate.notNull(instanceIds, "Instance id list is null!", new Object[0]);
        Validate.notEmpty(desiredState, "The desired state is empty!", new Object[0]);
        String collectiveInstanceState = "begin";
        int totalWaitTime = 0;
        if(instanceIds.size() == 0)
            return;
        while(!collectiveInstanceState.equalsIgnoreCase(desiredState)) 
        {
            List<Instance> currentInstanceList = getInstancesForIds(instanceIds);
            collectiveInstanceState = desiredState;
            for(Iterator<Instance> iterator = currentInstanceList.iterator(); iterator.hasNext();)
            {
                Instance instance = (Instance)iterator.next();
                if(instance != null && instance.getState() != null && !desiredState.equalsIgnoreCase(instance.getState().getName()))
                    collectiveInstanceState = instance.getState().getName();
            }

            logger.log((new StringBuilder("Instance state wait time: ")).append(totalWaitTime).append(" secs for state: ").append(desiredState).toString());
            Thread.sleep(10000L);
            if((totalWaitTime += 10) > maxWaitSecs)
            {
                logger.log((new StringBuilder("waitForInstanceState max wait time of ")).append(maxWaitSecs).append(" exceeded!").toString());
                throw new CPRuntimeException((new StringBuilder("waitForInstanceState max wait time of ")).append(maxWaitSecs).append(" secs exceeded!").toString());
            }
        }
    }

    public List<String> getInstanceIdList(List<Instance> instanceList)
    {
        List<String> instanceIds = new ArrayList<String>();
        for(Iterator<Instance> iterator = instanceList.iterator(); iterator.hasNext();)
        {
            Instance instance = (Instance)iterator.next();
            if(instance != null && instance.getInstanceId() != null)
                instanceIds.add(instance.getInstanceId());
        }

        return instanceIds;
    }

    protected List<String> getInstanceIdsForTag(String tagName, String tagValue)
    {
        List<String> instanceIds = new ArrayList<String>();
        List<Instance> instanceList = getInstancesForTag(tagName, tagValue);
        instanceIds = getInstanceIdList(instanceList);
        return instanceIds;
    }

    protected ArrayList<Instance> getInstancesFromResult(DescribeInstancesResult instanceResult)
    {
        ArrayList<Instance> instanceArray = new ArrayList<Instance>();
        List<Reservation> reservations = instanceResult.getReservations();
        Reservation reservation;
        for(Iterator<Reservation> iterator = reservations.iterator(); iterator.hasNext(); instanceArray.addAll(reservation.getInstances()))
            reservation = (Reservation)iterator.next();

        return instanceArray;
    }

    protected Instance getInstanceFromResult(DescribeInstancesResult result)
        throws MultipleObjectsReturnedException
    {
        List<Instance> allInstances = new ArrayList<Instance>();
        List<Reservation> reservations = result.getReservations();
        Reservation reservation;
        for(Iterator<Reservation> iterator = reservations.iterator(); iterator.hasNext(); allInstances.addAll(reservation.getInstances()))
            reservation = (Reservation)iterator.next();

        if(allInstances.size() > 1)
            throw new MultipleObjectsReturnedException();
        else
            return (Instance)allInstances.get(0);
    }


   	/**
   	 * checks to see if the shutdown before backup flag is set on an instance
   	 * 
   	 * @param instance
   	 * @return
   	 */
    public boolean amiBackup(Instance instance, String backupStrategyTag) {
    	
    	String backupStrategy = AWSInstanceEntity.getTagValueFromList(backupStrategyTag, instance.getTags(),"");
    	
        if(StringUtils.isEmpty(backupStrategy)) {
            return false;
        }
        else if (BACKUP_STRATEGY_AMI_CONST.equalsIgnoreCase(backupStrategy)) {
            return true;
        }
        return false;
    
    }
    
    public boolean snapshotRunningBackup(Instance instance, String backupStrategyTag) {
    	
    	String backupStrategy = AWSInstanceEntity.getTagValueFromList(backupStrategyTag, instance.getTags(),"");
  
        if(StringUtils.isEmpty(backupStrategy)) {
            return false;
        }
        else if (BACKUP_STRATEGY_SNAPSHOT_RUNNING_CONST.equalsIgnoreCase(backupStrategy)) {
            return true;
        }
        return false;
    
    }

}
