package com.awsamazon.external;

import com.amazonaws.auth.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

/**
 * Created by srramas on 7/6/17.
 */
final public class MyAWSCredentialsProvider implements AWSCredentialsProvider, Configurable {

    private static final Log LOG = LogFactory.getLog(MyAWSCredentialsProvider.class);

    private Configuration configuration;
    private static AWSCredentials credentials;

    public MyAWSCredentialsProvider(URI uri, Configuration conf) {
        this.configuration = conf;

       String accesskey= conf.get("aws.accesskey");
       String secretaccesskey= conf.get("aws.secretkey");
       String sessionkey=conf.get("aws.sessionkey");

       if(StringUtils.isNotEmpty(accesskey) &&
               StringUtils.isNotEmpty(accesskey)&&StringUtils.isNotEmpty(accesskey)){

           LOG.info("S3 custom credential provided");

            BasicSessionCredentials temporaryCredentials =
                    new BasicSessionCredentials(
                            accesskey,
                           secretaccesskey,
                            sessionkey);
            credentials = temporaryCredentials;
        } else {
            //Extracting the credentials from EC2 metadata service
            Boolean refreshCredentialsAsync = true;
            InstanceProfileCredentialsProvider creds = new InstanceProfileCredentialsProvider
                    (refreshCredentialsAsync);
            credentials = creds.getCredentials();
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        //Returning the credentials to EMRFS to make S3 API calls
        return credentials;
    }

    @Override
    public void refresh() {}

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

}
