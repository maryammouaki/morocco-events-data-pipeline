package com.morocco.events.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CityStatistics implements Serializable {
    private String city;
    private int totalEvents;
    private double maxPrice;
    private double minPrice;
    private Map<String, Double> categoryTotals;

    public CityStatistics() {
        this.categoryTotals = new HashMap<>();
    }

    public CityStatistics(String city) {
        this.city = city;
        this.totalEvents = 0;
        this.maxPrice = 0.0;
        this.minPrice = Double.MAX_VALUE;
        this.categoryTotals = new HashMap<>();
    }

    // Getters et Setters
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public int getTotalEvents() { return totalEvents; }
    public void setTotalEvents(int totalEvents) { this.totalEvents = totalEvents; }

    public double getMaxPrice() { return maxPrice; }
    public void setMaxPrice(double maxPrice) { this.maxPrice = maxPrice; }

    public double getMinPrice() { return minPrice; }
    public void setMinPrice(double minPrice) { this.minPrice = minPrice; }

    public Map<String, Double> getCategoryTotals() { return categoryTotals; }
    public void setCategoryTotals(Map<String, Double> categoryTotals) {
        this.categoryTotals = categoryTotals;
    }

    @Override
    public String toString() {
        return city + ": total=" + totalEvents + 
               ", maxPrice=" + String.format("%.2f", maxPrice) + 
               " MAD, minPrice=" + String.format("%.2f", minPrice) + 
               " MAD, categories=" + formatCategories();
    }

    private String formatCategories() {
        StringBuilder sb = new StringBuilder("{");
        categoryTotals.forEach((category, total) -> {
            if (sb.length() > 1) sb.append(", ");
            sb.append(category).append(":").append(String.format("%.2f", total));
        });
        sb.append("}");
        return sb.toString();
    }
}
