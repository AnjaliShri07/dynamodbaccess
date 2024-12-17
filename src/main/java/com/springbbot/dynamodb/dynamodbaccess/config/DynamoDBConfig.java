package com.springbbot.dynamodb.dynamodbaccess.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

@Configuration
public class DynamoDBConfig {

   /* @Value("${aws.dynamodb.endpoint}")
    private String endpoint;

    @Value("${aws.dynamodb.access-key}")
    private String accessKey;

    @Value("${aws.dynamodb.secret-key}")
    private String secretKey;*/

    @Bean
    public DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("endpoint")) // For LocalStack or Local DynamoDB
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials
                                .create("accessKey",
                                        "secretKey")))
                .build();
    }

    //To read default AWS credentials provider chain
    /*@Bean
    public DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }*/
}
