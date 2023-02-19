package org.ddbninja;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.ddbninja.model.Movie;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class App
{
    public static void main( String[] args ) throws IOException {
        DynamoDB dynamoDB = getDynamoDB();
        AmazonDynamoDB amazonDynamoDBClient = getAmazonDynamoDBClient();
//        loadMoviesData(dynamoDB);
//        BatchWriteItemOutcome moviesBatchWriteItemOutcome = putMovieItem(dynamoDB, 2023, "The Impeding Reckoning");
//        BatchWriteItemResult moviesBatchWriteItemResult = moviesBatchWriteItemOutcome.getBatchWriteItemResult();
//        System.out.println("Movie batch write result: " + moviesBatchWriteItemResult.toString());
//        System.out.println("Movie batch write result consumed capacity: " + moviesBatchWriteItemResult.getConsumedCapacity());

        ItemCollection<QueryOutcome> moviesFrom2023 = queryMoviesTableByYear(dynamoDB, 2023);

        try {
            moviesFrom2023.forEach(item -> {
                System.out.println(item.getNumber("year") + ": " + item.getString("title"));
            });
        } catch (Exception e) {
            System.err.println("Unable to query movies from year 2023: " + e.getMessage());
        }

//        String shareIterator = getDynamoDBStreamShardsIterator(getDynamoDbStreamsClient(), "Movies");
//        List<Record> records = getStreamShardRecords(getDynamoDbStreamsClient(), shareIterator);
//
//        for (Record record : records) {
//            System.out.println("Record: " + record.getEventName());
//            System.out.println("Old Image: " + record.getDynamodb().getOldImage().toString());
//            System.out.println("New Image: " + record.getDynamodb().getNewImage().toString());
//            System.out.println();
//        }

        Movie movie = new Movie();
        movie.setYear(2011);
        movie.setTitle("2 Guns");

        Movie movie2 = new Movie();
        movie2.setYear(2023);
        movie2.setTitle("Mastering the Art of Deception");

        Movie movieUpdate = new Movie();
        movieUpdate.setYear(2011);
        movieUpdate.setTitle("2 Guns");

        List<String> actors = new ArrayList<>(Arrays.asList("Mark Wahlberg", "Denzel Washington", "Paula Patton"));
        movieUpdate.setActors(actors);

        reviewMovieItem(amazonDynamoDBClient, movie);
        updateMovieItem(amazonDynamoDBClient, movieUpdate);
        reviewMovieItem(amazonDynamoDBClient, movieUpdate);

        reviewMovieItems(amazonDynamoDBClient, new ArrayList<>(Arrays.asList(movie, movie2)));

        dynamoDB.shutdown();
        amazonDynamoDBClient.shutdown();
    }

    /**
     * Provides configuration setting for {@link AmazonDynamoDB} and {@link AmazonDynamoDBStreams}
     * clients. Configurations come with default settings for SDK retry, error, and timeout logic,
     * as a few examples, though custom configurations provide a way to customize this SDK logic
     * based on your application needs.
     *
     * @return custom {@link ClientConfiguration}
     */
    public static ClientConfiguration dynamoClientConfiguration() {
        return new ClientConfiguration()
                .withConnectionTimeout(120)
                .withMaxErrorRetry(3)
                .withThrottledRetries(true);
    }

    public static DynamoDB getDynamoDB() {
        AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withClientConfiguration(dynamoClientConfiguration())
                .build();

        return new DynamoDB(ddbClient);
    }

    public static AmazonDynamoDB getAmazonDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withClientConfiguration(dynamoClientConfiguration())
                .build();
    }

    public static AmazonDynamoDBStreams getDynamoDbStreamsClient() {
        return AmazonDynamoDBStreamsClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withClientConfiguration(dynamoClientConfiguration())
                .build();
    }

    public static void loadMoviesData(DynamoDB dynamoDB) throws IOException {
        Table moviesTable = dynamoDB.getTable("Movies");

        JsonParser parser = new JsonFactory().createParser(new File("/home/tuvok/ddb/moviedata.json"));

        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> iterator = rootNode.iterator();

        ObjectNode currentNode;

        while (iterator.hasNext()) {
            currentNode = (ObjectNode) iterator.next();

            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            try {
                moviesTable.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON(
                        "info", currentNode.path("info").toString()));

                System.out.println("PutItem succeeded: " + year + " " + title);
            } catch (Exception e) {
                System.err.println("error adding movie: " + year + " " + title);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();
    }

    public static BatchWriteItemOutcome putMovieItem(DynamoDB dynamoDB, int year, String title) {
        TableWriteItems tableWriteItems = new TableWriteItems("Movies")
                .withItemsToPut(new Item().withPrimaryKey("year", year, "title", title));
        return dynamoDB.batchWriteItem(tableWriteItems);
    }

    public static ItemCollection<QueryOutcome> queryMoviesTableByYear(DynamoDB dynamoDB, int year) {
        Table moviesTable = dynamoDB.getTable("Movies");

        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", year);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        return moviesTable.query(querySpec);
    }

    public static  String getDynamoDBStreamShardsIterator(AmazonDynamoDBStreams dynamoDBStreams, String tableName) {
        Stream stream = new Stream().withTableName(tableName);
        String streamArn = stream.getStreamArn();
        DescribeStreamResult streamResult = dynamoDBStreams.describeStream(
                new DescribeStreamRequest().withStreamArn(streamArn));

        // get first shard
        Shard shard = streamResult.getStreamDescription().getShards().get(0);

        GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardIteratorType(ShardIteratorType.LATEST)
                .withShardId(shard.getShardId());

        GetShardIteratorResult result = dynamoDBStreams.getShardIterator(shardIteratorRequest);
        return result.getShardIterator();
    }

    public static List<Record> getStreamShardRecords(AmazonDynamoDBStreams streamsClient, String shardIterator) {
        GetRecordsResult recordsResult = streamsClient.getRecords(new GetRecordsRequest()
                .withShardIterator(shardIterator));
        return recordsResult.getRecords();
    }

    /**
     * Example of using DynamoDB's Object Persistence Interface with {@link DynamoDBMapper}.
     * This maps {@link Movie} object to the DynamoDB Movie table. Supplying the HASH and Range
     * keys for a specific movie item allows us to review that item and it's attributes in full.
     *
     * @param dynamoDBClient {@link AmazonDynamoDB}
     * @param movieItem {@link Movie}
     */
    public static void reviewMovieItem(AmazonDynamoDB dynamoDBClient, Movie movieItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBClient);

        try {
            Movie item = mapper.load(movieItem, DynamoDBMapperConfig.DEFAULT);
            if (item != null) {
                List<String> actors = item.getActors();
                System.out.printf("%s was released in %d%n", item.getTitle(), item.getYear());

                if (item.getActors() != null) {
                    for (String actor: actors) {
                        System.out.printf("actor: %s%n", actor);
                    }
                }

            } else {
                System.out.printf("No result was found for movie: %s%n", movieItem.getTitle());
            }
        } catch (Exception e) {
            System.out.printf("Error querying movie: %s, %s%n", movieItem.getTitle(), e.getMessage());
        }
    }

    public static void reviewMovieItems(AmazonDynamoDB amazonDynamoDBClient, List<Movie> movies) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDBClient);

        try {
            Map<String, List<Object>> items = mapper.batchLoad(movies);
            List<Object> moviesList = items.get("Movies");

            // supports type casting to original item class
            for (Object obj : moviesList) {
                Movie movie = (Movie) obj;
                System.out.println("movie: " + movie.getTitle());
                System.out.println("year: " + movie.getYear());
                if (movie.getActors() != null) {
                    System.out.println(movie.getTitle() + " has a list of actors: ");
                    movie.getActors().forEach(actor -> System.out.println("actor: " + actor));
                }
            }

        } catch (Exception e) {
            System.out.println("error fetching movies: " + e.getMessage());
        }
    }

    /**
     * Update a movie with the same HASH and RANGE keys, passing a {@link Movie} object with updated fields.
     * {@link DynamoDBMapper#save(Object)} can take a {@link DynamoDBMapperConfig} that can change save pattern
     * as {@link AmazonDynamoDB#putItem(PutItemRequest)} or {@link AmazonDynamoDB#updateItem(UpdateItemRequest)}.
     *
     * @param amazonDynamoDB {@link AmazonDynamoDB}
     * @param movieItem {@link Movie} Object
     */
    public static void updateMovieItem(AmazonDynamoDB amazonDynamoDB, Movie movieItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        try {
            mapper.save(movieItem);
            System.out.printf("Saved %s%n", movieItem.getTitle());
        } catch (Exception e) {
            System.out.printf("Error saving movie: %s, %s%n", movieItem.getTitle(), e.getMessage());
        }
    }
}
