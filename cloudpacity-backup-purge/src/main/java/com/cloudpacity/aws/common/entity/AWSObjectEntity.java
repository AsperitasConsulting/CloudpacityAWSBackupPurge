package com.cloudpacity.aws.common.entity;
// Decompiled by Jad v1.5.8e. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.geocities.com/kpdus/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   AWSObjectEntity.java



import java.util.List;

import org.apache.commons.lang3.Validate;

import com.amazonaws.services.ec2.model.Tag;
import com.cloudpacity.aws.common.CPCommonEnv;
import com.cloudpacity.aws.common.util.CPLogger;

public class AWSObjectEntity
{
	protected CPLogger logger = null;	
	protected CPCommonEnv env;

	
	public AWSObjectEntity (CPLogger logger,CPCommonEnv env) {
		Validate.notNull(env, "The environment parms are null!");	
		Validate.notNull(logger, "The logger is null!");	
		
		this.logger =  logger;
		this.env = env;
	}
	
	/**
	 * 
	 * @param tagName
	 * @param tags
	 * @return
	 */
	public static Tag getTagFromList(String tagName, List<Tag> tags) {
		
		Validate.notEmpty(tagName, "The tag name give was empty or null");
		Validate.notNull(tags, "the tag list given was null");
		
		for (Tag tag:tags) {
			if (tagName.equalsIgnoreCase(tag.getKey())) {
				return tag;
			}
		}
		
		return null;
	}
	
	/**
	 * 
	 * @param tagName
	 * @param tags
	 * @param defaultValue
	 * @return
	 */
	public static String getTagValueFromList(String tagName, List<Tag> tags, String defaultValue) {
		
		Validate.notNull(tagName, "The tag name give was null");
		Validate.notNull(tags, "the tag list given was null");
		
		Tag tag = getTagFromList(tagName, tags);
		
		if (tag != null) {
			return tag.getValue();
		}
		else {
			return defaultValue;
		}
		
		
	}
}
