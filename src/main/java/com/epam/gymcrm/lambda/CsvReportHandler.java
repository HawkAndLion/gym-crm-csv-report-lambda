package com.epam.gymcrm.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;

public class CsvReportHandler implements RequestHandler<Object, Map<String, Object>> {

    private final DynamoDbClient dynamoDb = DynamoDbClient.create();
    private final S3Client s3 = S3Client.create();

    private final String tableName = System.getenv("DYNAMODB_TABLE");
    private final String bucketName = System.getenv("REPORT_BUCKET");

    @Override
    public Map<String, Object> handleRequest(Object input, Context context) {

        YearMonth currentMonth = YearMonth.now();
        Map<String, TrainerSummary> summaryMap = new HashMap<>();

        ScanResponse response = dynamoDb.scan(
                ScanRequest.builder().tableName(tableName).build()
        );

        for (Map<String, AttributeValue> item : response.items()) {

            String trainingDate = item.get("trainingDate").s();
            if (!YearMonth.from(LocalDate.parse(trainingDate)).equals(currentMonth)) {
                continue;
            }

            long duration = Long.parseLong(item.get("durationMinutes").n());
            if (duration <= 0) {
                continue;
            }

            String username = item.get("username").s();

            summaryMap.computeIfAbsent(username, u ->
                    new TrainerSummary(
                            item.get("firstName").s(),
                            item.get("lastName").s()
                    )
            ).addMinutes(duration);
        }

        StringBuilder csv = new StringBuilder();
        csv.append("Trainer First Name,Trainer Last Name,Current Month Trainings Duration (minutes)\n");

        for (TrainerSummary ts : summaryMap.values()) {
            if (ts.totalMinutes > 0) {
                csv.append(ts.firstName).append(",")
                        .append(ts.lastName).append(",")
                        .append(ts.totalMinutes).append("\n");
            }
        }

        String fileName = String.format(
                "Trainers_Trainings_summary_%d_%02d.csv",
                currentMonth.getYear(),
                currentMonth.getMonthValue()
        );

        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(fileName)
                        .contentType("text/csv")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(
                        csv.toString().getBytes(StandardCharsets.UTF_8)
                )
        );

        return Map.of(
                "statusCode", 200,
                "fileName", fileName
        );
    }

    static class TrainerSummary {
        String firstName;
        String lastName;
        long totalMinutes = 0;

        TrainerSummary(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        void addMinutes(long minutes) {
            totalMinutes += minutes;
        }
    }
}
