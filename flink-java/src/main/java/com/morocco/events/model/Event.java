package com.morocco.events.model;

import java.io.Serializable;
import java.util.Objects;

public class Event implements Serializable {
    private String title;
    private String city;
    private double price;
    private String category;
    private String source;
    private long timestamp;

    // Constructeur vide (IMPORTANT pour Flink)
    public Event() {}

    // Constructeur avec paramètres
    public Event(String title, String city, double price, 
                 String category, String source, long timestamp) {
        this.title = title;
        this.city = city;
        this.price = price;
        this.category = category;
        this.source = source;
        this.timestamp = timestamp;
    }

    // Getters
    public String getTitle() { return title; }
    public String getCity() { return city; }
    public double getPrice() { return price; }
    public String getCategory() { return category; }
    public String getSource() { return source; }
    public long getTimestamp() { return timestamp; }

    // Setters
    public void setTitle(String title) { this.title = title; }
    public void setCity(String city) { this.city = city; }
    public void setPrice(double price) { this.price = price; }
    public void setCategory(String category) { this.category = category; }
    public void setSource(String source) { this.source = source; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Event{" +
                "title='" + title + '\'' +
                ", city='" + city + '\'' +
                ", price=" + price +
                ", category='" + category + '\'' +
                ", source='" + source + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Double.compare(event.price, price) == 0 &&
                timestamp == event.timestamp &&
                Objects.equals(title, event.title) &&
                Objects.equals(city, event.city) &&
                Objects.equals(category, event.category) &&
                Objects.equals(source, event.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, city, price, category, source, timestamp);
    }
}
