package com.awsamazon.external;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Created by srramas on 7/6/17.
 */
final public class MyAWSCredentialsProvider implements AWSCredentialsProvider, Configurable {

    private static final Log LOG = LogFactory.getLog(MyAWSCredentialsProvider.class);
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .build();
    private Configuration configuration;
    private static AWSCredentials credentials;
    private  String prefix="aws157";
    private boolean emrlog=false;
    private String s3path="";

    public MyAWSCredentialsProvider(URI uri, Configuration conf) {
        this.configuration = conf;
        emrlog= conf.getBoolean("aws.emrlog.enabled",false);
        prefix= conf.get("aws.emrlog.prefix","aws157");
        s3path=uri.toString();
        selectCredential();

    }

    private void selectCredential() {
        if(emrlog && s3path.contains(prefix)){

           LOG.debug("S3 custom credential provided");
            S3Object fullObject = s3Client.getObject(new GetObjectRequest("depedentjars", "emrfs/credential.json"));
            ObjectMapper mapper = new ObjectMapper();
            try {
                Map<String, String> map = mapper.readValue(fullObject.getObjectContent(), Map.class);
                BasicSessionCredentials temporaryCredentials =
                        new BasicSessionCredentials(
                                map.get("accessKey"),
                                map.get("secretKey"),
                                map.get("sessionKey"));
                credentials = temporaryCredentials;
            } catch (IOException e) {
                e.printStackTrace();
            }

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
    public void refresh() {
        selectCredential();
    }

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

}
