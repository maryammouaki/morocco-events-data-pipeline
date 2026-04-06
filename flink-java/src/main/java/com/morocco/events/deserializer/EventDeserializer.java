package com.morocco.events.deserializer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.morocco.events.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class EventDeserializer implements DeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;
    private transient Gson gson;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.gson = new Gson();
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        try {
            String jsonString = new String(message);
            com.google.gson.JsonObject json = gson.fromJson(jsonString, com.google.gson.JsonObject.class);
            
            Event event = new Event();
            event.setTitle(json.has("title") ? json.get("title").getAsString() : "Unknown");
            event.setCity(json.has("city") ? json.get("city").getAsString() : "Unknown");
            event.setPrice(json.has("price") ? json.get("price").getAsDouble() : 0.0);
            event.setCategory(json.has("category") ? json.get("category").getAsString() : "Unknown");
            event.setSource(json.has("source") ? json.get("source").getAsString() : "unknown");
            
            // Try multiple timestamp field names
            long timestamp = 0;
            if (json.has("timestamp") && !json.get("timestamp").isJsonNull()) {
                timestamp = json.get("timestamp").getAsLong();
            } else if (json.has("startAt") && !json.get("startAt").isJsonNull()) {
                // Try to parse ISO format date
                try {
                    String dateStr = json.get("startAt").getAsString();
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    timestamp = sdf.parse(dateStr).getTime();
                } catch (Exception e) {
                    timestamp = System.currentTimeMillis();
                }
            } else if (json.has("start_date") && !json.get("start_date").isJsonNull()) {
                try {
                    String dateStr = json.get("start_date").getAsString();
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                    timestamp = sdf.parse(dateStr).getTime();
                } catch (Exception e) {
                    timestamp = System.currentTimeMillis();
                }
            } else {
                timestamp = System.currentTimeMillis();
            }
            
            event.setTimestamp(timestamp);
            return event;
        } catch (Exception e) {
            throw new IOException("Failed to deserialize JSON message: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
