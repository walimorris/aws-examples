package org.ddbninja.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.List;

@DynamoDBTable(tableName = "Movies")
public class Movie {

    private int year;
    private String title;
    private List<String> actors;

    @DynamoDBHashKey(attributeName = "year")
    public int getYear() {
        return this.year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @DynamoDBRangeKey(attributeName = "title")
    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @DynamoDBAttribute(attributeName = "actors")
    public List<String> getActors() {
        return this.actors;
    }

    public void setActors(List<String> actors) {
        this.actors = actors;
    }
}
