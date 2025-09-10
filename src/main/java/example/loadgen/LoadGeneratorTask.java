package example.loadgen;

import static example.loadgen.LoadGeneratorConnectorConfig.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class LoadGeneratorTask extends SourceTask {

    private static final String STRING_COLUMN = "string_column";
    private static final String NUMERIC_COLUMN = "numeric_column";
    private static final String BOOLEAN_COLUMN = "boolean_column";

    private final Random random = new Random(System.currentTimeMillis());
    private final Logger log = LoggerFactory.getLogger(LoadGeneratorTask.class);

    private LoadGeneratorConnectorConfig config;
    private int messagesPerSecond;
    private int messageSizeBytes;
    private double availableTokens;
    private long lastRefillTimeMs;
    private long startTimeMs;
    private long lastLogSecond;
    private int eventsSentThisSecond;
    private long totalEventsSent;
    private int maxDurationSeconds; // 0 or negative = unlimited
    private long maxMessagesToSend; // 0 = unlimited; computed as messagesPerSecond * maxDurationSeconds
    private boolean stopLogged;
    private static final java.util.Map<String, String> SOURCE_PARTITION = java.util.Collections.singletonMap("source", "source-1");
    private String outputTopic;
    private Schema recordSchema;
    private boolean runIndefinitely;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new LoadGeneratorConnectorConfig(properties);
        messagesPerSecond = config.getInt(TASK_MESSAGES_PER_SECOND_CONFIG);
        if (messagesPerSecond <= 0) messagesPerSecond = 1;
        messageSizeBytes = config.getInt(MESSAGE_SIZE_BYTES_CONFIG);
        if (messageSizeBytes <= 0) messageSizeBytes = 1;
        availableTokens = 0.0;
        startTimeMs = System.currentTimeMillis();
        lastRefillTimeMs = startTimeMs;
        lastLogSecond = startTimeMs / 1000L;
        eventsSentThisSecond = 0;
        totalEventsSent = 0L;
        runIndefinitely = config.getBoolean(TASK_RUN_INDEFINITELY_CONFIG);
        maxDurationSeconds = config.getInt(TASK_MAX_DURATION_SECONDS_CONFIG);
        if (!runIndefinitely && maxDurationSeconds > 0 && messagesPerSecond > 0) {
            maxMessagesToSend = (long) messagesPerSecond * (long) maxDurationSeconds;
        } else {
            maxMessagesToSend = 0L; // unlimited
        }
        stopLogged = false;
        outputTopic = config.getString(OUTPUT_TOPIC_CONFIG);
        recordSchema = SchemaBuilder.struct()
            .field(STRING_COLUMN, Schema.STRING_SCHEMA).required()
            .field(NUMERIC_COLUMN, Schema.INT32_SCHEMA).required()
            .field(BOOLEAN_COLUMN, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long nowMs = System.currentTimeMillis();
        if (maxMessagesToSend > 0 && totalEventsSent >= maxMessagesToSend) {
            if (!stopLogged) {
                long elapsedTotalSeconds = (nowMs - startTimeMs) / 1000L;
                log.info("Load generator finished: elapsed={}s, target_messages={}, total_sent={}, stopped",
                    elapsedTotalSeconds, maxMessagesToSend, totalEventsSent);
                stopLogged = true;
            }
            Thread.sleep(250);
            return Collections.emptyList();
        }

        double elapsedSeconds = (nowMs - lastRefillTimeMs) / 1000.0;
        if (elapsedSeconds > 0) {
            availableTokens = Math.min(messagesPerSecond, availableTokens + elapsedSeconds * messagesPerSecond);
            lastRefillTimeMs = nowMs;
        }

        int toSend = (int) Math.floor(availableTokens);
        if (maxMessagesToSend > 0) {
            long remaining = maxMessagesToSend - totalEventsSent;
            if (remaining <= 0) {
                toSend = 0;
            } else if (toSend > remaining) {
                toSend = (int) Math.min(remaining, Integer.MAX_VALUE);
            }
        }
        if (toSend <= 0) {
            Thread.sleep(Math.max(1, 1000 / Math.max(1, messagesPerSecond)));
            return Collections.emptyList();
        }

        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < toSend; i++) {
            long nextOffset = totalEventsSent + 1;
            records.add(new SourceRecord(
                SOURCE_PARTITION,
                java.util.Collections.singletonMap("offset", nextOffset),
                outputTopic, null, null, null,
                recordSchema, createStruct(recordSchema)));
            eventsSentThisSecond++;
            totalEventsSent++;
        }
        availableTokens -= toSend;

        long currentSecond = nowMs / 1000L;
        if (currentSecond > lastLogSecond) {
            long totalElapsed = (nowMs - startTimeMs) / 1000L;
            log.info("Load generator stats: elapsed={}s, sent_last_second={}, total_sent={}, mps_config={} message_size_bytes={}",
                totalElapsed, eventsSentThisSecond, totalEventsSent, messagesPerSecond, messageSizeBytes);
            eventsSentThisSecond = 0;
            lastLogSecond = currentSecond;
        }
        return records;
    }

    private Struct createStruct(Schema schema) {
        Struct struct = new Struct(schema);
        struct.put(STRING_COLUMN, sizedString(messageSizeBytes));
        struct.put(NUMERIC_COLUMN, random.nextInt(1000));
        struct.put(BOOLEAN_COLUMN, random.nextBoolean());
        return struct;
    }

    private String sizedString(int size) {
        if (size <= 0) return "";
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 'A');
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    @Override
    public void stop() {}
}


