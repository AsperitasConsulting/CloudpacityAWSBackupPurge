package com.cloudpacity.aws.purge.lambda;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.cloudpacity.aws.common.util.CPLogger;
import com.cloudpacity.aws.purge.pojo.BackupPurgeRequest;
import com.cloudpacity.aws.purge.service.CPBackupPurge;

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
public class CPBackupPurgeLambda  implements RequestHandler<BackupPurgeRequest, String> {

    protected AWSCredentials awsCredentials;
    protected CPLogger logger;
    
    public CPBackupPurgeLambda()    {
        awsCredentials = (new EnvironmentVariableCredentialsProvider()).getCredentials();
    }

    public String handleRequest(BackupPurgeRequest request, Context context)
    {
    	try {
	        logger = new CPLogger(context.getLogger());
	        logger.log("BEGIN BACKUP PROCESS");

	        if(request == null)
	            request = new BackupPurgeRequest();
	        
	        request.setAwsAccountId(context.getInvokedFunctionArn().split(":")[4]);

	        validateBackupPurgeRequest(request, context);
	        logger.log(request.toString());
	        
	        CPBackupPurge backupPurge = new CPBackupPurge(logger, awsCredentials);
	        return backupPurge.invoke(request);
		}
	   	catch (Throwable e){
	   		logger.log("Exception in EC2backup.handleRequest!");
	   		logger.log(logger.getDebugMessages());
	   		logger.log(e.getMessage());
	   		logger.log(ExceptionUtils.getStackTrace(e));
	   		return e.getMessage();
	   }  
    }
    
    private void validateBackupPurgeRequest(BackupPurgeRequest request, Context context)
    {
        Validate.notNull(request, "The backup request is null!", new Object[0]);
        Validate.notNull(context, "The Lambda context is null!", new Object[0]);

        
    }

}
