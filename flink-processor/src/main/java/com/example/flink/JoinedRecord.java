package com.example.flink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class JoinedRecord {
    @JsonIgnore
    String id;
    String typename;
    double temperature;
    double humidity;
    @JsonIgnore
    double timestamp;

    public JoinedRecord() {
    }

    public JoinedRecord(String id, String typename, double temperature, double humidity, double timestamp) {
        this.id = id;
        this.typename = typename;
        this.temperature = temperature;
        this.humidity = humidity;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTypename() {
        return typename;
    }

    public void setTypename(String typename) {
        this.typename = typename;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "JoinedRecord{" +
                "id='" + id + '\'' +
                ", typename='" + typename + '\'' +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", timestamp=" + timestamp +
                '}';
    }


    static private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm");
    static private final ZoneId zone = ZoneId.systemDefault();

    @JsonProperty("time")
    public String getJsonTime() {
        long millis = (long) (timestamp * 1000);
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zone);
        return dateTime.format(timeFormatter);
    }
}
