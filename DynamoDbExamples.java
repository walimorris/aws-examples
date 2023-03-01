package org.ddbninja;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.istack.internal.NotNull;
import org.ddbninja.model.Movie;
import org.ddbninja.model.Streams;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class App {

    public static final String TITLE_INDEX = "title-index";
    public static final String MOVIES_TABLE = "Movies";
    public static final String STREAMS_TABLE = "streams-table";

    public static void main( String[] args ) throws IOException {
        DynamoDB dynamoDB = getDynamoDB();
        AmazonDynamoDB amazonDynamoDBClient = getAmazonDynamoDBClient();
//        loadMoviesData(dynamoDB);
//        BatchWriteItemOutcome moviesBatchWriteItemOutcome = putMovieItem(dynamoDB, 2023, "The Impeding Reckoning");
//        BatchWriteItemResult moviesBatchWriteItemResult = moviesBatchWriteItemOutcome.getBatchWriteItemResult();
//        System.out.println("Movie batch write result: " + moviesBatchWriteItemResult.toString());
//        System.out.println("Movie batch write result consumed capacity: " + moviesBatchWriteItemResult.getConsumedCapacity());

//        ItemCollection<QueryOutcome> moviesFrom2023 = queryMoviesTableByYear(dynamoDB, 2023);
//
//        try {
//            moviesFrom2023.forEach(item -> {
//                System.out.println(item.getNumber("year") + ": " + item.getString("title"));
//            });
//        } catch (Exception e) {
//            System.err.println("Unable to query movies from year 2023: " + e.getMessage());
//        }

//        String shareIterator = getDynamoDBStreamShardsIterator(getDynamoDbStreamsClient(), "Movies");
//        List<Record> records = getStreamShardRecords(getDynamoDbStreamsClient(), shareIterator);
//
//        for (Record record : records) {
//            System.out.println("Record: " + record.getEventName());
//            System.out.println("Old Image: " + record.getDynamodb().getOldImage().toString());
//            System.out.println("New Image: " + record.getDynamodb().getNewImage().toString());
//            System.out.println();
//        }

//        Movie movie = new Movie();
//        movie.setYear(2011);
//        movie.setTitle("2 Guns");
//
//        Movie movie2 = new Movie();
//        movie2.setYear(2023);
//        movie2.setTitle("Mastering the Art of Deception");
//
//        Movie movieUpdate = new Movie();
//        movieUpdate.setYear(2011);
//        movieUpdate.setTitle("2 Guns");
//
//        List<String> actors = new ArrayList<>(Arrays.asList("Mark Wahlberg", "Denzel Washington", "Paula Patton"));
//        movieUpdate.setActors(actors);
//
//        reviewMovieItem(amazonDynamoDBClient, movie);
//        updateMovieItem(amazonDynamoDBClient, movieUpdate);
//        reviewMovieItem(amazonDynamoDBClient, movieUpdate);
//
//        reviewMovieItems(amazonDynamoDBClient, new ArrayList<>(Arrays.asList(movie, movie2)));

        // example querying a global secondary index by only supplying the index's HASH Key
        // This returns a Movie object along with the projected attributes from the base table
//        Movie movie = new Movie();
//        movie.setTitle("2 Guns");
//
//        Movie result = queryMovieByTitle(amazonDynamoDBClient, movie);
//        System.out.println(result.getTitle());
//        System.out.println(result.getYear());
//        System.out.println(result.getActors().toString());
//
//        List<Movie> queryResult = queryMoviesByYear(amazonDynamoDBClient, 2023);
//        System.out.println("results:");
//        for (Movie m : queryResult) {
//            System.out.println(m.getTitle());
//        }
//
//        // After creating a global table, add item to base table and review replication
//        // Go further by deleting that item and reviewing the replication change, is the
//        // item deleted on the replicated tables. If so, was it fast?
//        Movie replicateMovie = new Movie();
//        replicateMovie.setYear(2023);
//        replicateMovie.setTitle("JUNG_E");
//        replicateMovie.setActors(new ArrayList<>(Arrays.asList("Kang Soo-yeon", "Kim Hyun-joo", "Ryu Kyung-soo", "Uhm Ji-won")));
//
//        // put the item if it does not exist
//        boolean loadedResult = putMovieItem(amazonDynamoDBClient, replicateMovie);
//        System.out.println(replicateMovie.getTitle() + " loaded = " + loadedResult);

//        List<Movie> moviesByYearAndFirstLetterResult = queryMoviesByYear(amazonDynamoDBClient, 2023);
//        System.out.println(moviesByYearAndFirstLetterResult.size());

//        Streams streamsItem = new Streams();
//        streamsItem.setMachineId(654321);
//        streamsItem.setMachineType("Adafruit");
//
//        Streams streamsItemResult = loadStreamsItemByMachineIdAndType(amazonDynamoDBClient, streamsItem);
//        System.out.printf("MachineId=%d, MachineType=%s, Temperature=%s%n", streamsItemResult.getMachineId(), streamsItemResult.getMachineType(), streamsItemResult.getTemperature());
//        boolean gsiCreated = createGlobalSecondaryIndexOnStreamsTable(amazonDynamoDBClient, "temperature", null);
//        System.out.println("GSI Created: " + gsiCreated);

//        // forced Exception
//        boolean gsi2Created = createGlobalSecondaryIndexOnStreamsTable(amazonDynamoDBClient, null, null);

        Streams streamItem = new Streams();
        streamItem.setTemperature("66.4");
        PaginatedQueryList<Streams> queryListResult = queryStreamsTemperatureIndexByTemperature(amazonDynamoDBClient, streamItem);

        System.out.println(queryListResult.size());
        queryListResult.forEach((result -> {
            System.out.println("machineId=" + result.getMachineId() + " machineType=" + result.getMachineType() + " temperature=" + result.getTemperature());
        }));

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

            // supports type casting to original class
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

    /**
     * Puts a {@link Movie} Item to the Movies Table with a conditional save behavior of adding the movie only
     * if the movie item is not present on the table. DynamoDB has added best practices embedded by having the
     * default put item operation to update(overwrite) a matched item. This function goes further by adding a
     * condition that checks if the item is present and if so, doesn't overwrite the item. Read Capacity Units
     * are consumed, but this saves on Write Capacity Units from ignoring a update (overwrite) of an item.
     *
     * @param amazonDynamoDB {@link AmazonDynamoDB}
     * @param movieItem {@link Movie}
     *
     * @return boolean
     */
    public static boolean putMovieItem(AmazonDynamoDB amazonDynamoDB, Movie movieItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.PUT)
                .build();

        try {
            Movie result = loadMovieByYear(amazonDynamoDB, movieItem);
            if (result == null) {
                mapper.save(movieItem, config);
            } else {
                System.out.printf("Movie: %s, exists in table.%n", movieItem.getTitle());
                return false;
            }
        } catch (Exception e) {
            System.out.printf("Error saving movie: %s, %s%n", movieItem.getTitle(), e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * <p>
     * Queries the {@link Movie} table using movie title by querying from the table's global
     * secondary index by using {@link DynamoDBQueryExpression#withIndexName(String)} function.
     * <p>
     * Further, the secondary index HASH and/or RANGE key(s) must be properly annotated in
     * the underlying {@link Movie} object model.
     *
     * @see Movie
     *
     * @param amazonDynamoDB {@link AmazonDynamoDB} client
     * @param movieItem {@link Movie}
     *
     * @return {@link Movie} and projected attributes
     */
    public static Movie queryMovieByTitle(AmazonDynamoDB amazonDynamoDB, Movie movieItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        DynamoDBQueryExpression<Movie> queryExpression = new DynamoDBQueryExpression<Movie>()
                .withHashKeyValues(movieItem)
                .withLimit(1)
                .withIndexName(TITLE_INDEX)
                .withConsistentRead(false);

        return mapper.query(Movie.class, queryExpression).get(0);
    }

    /**
     * Queries the {@link Movie} table by year utilizing the HASH KEY of a {@link Movie} object.
     * This allows the function to query all movies by year and sets a limit of 10 return items.
     * The {@link DynamoDBMapper} assists with querying by mapping the results to the given
     * class passed to the {@link DynamoDBMapper#query(Class, DynamoDBQueryExpression)} function.
     *
     * @param amazonDynamoDB {@link AmazonDynamoDB} client
     * @param year int
     *
     * @return {@link List}
     */
    public static List<Movie> queryMoviesByYear(AmazonDynamoDB amazonDynamoDB, int year) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        // movie with HASH value
        Movie movie = new Movie();
        movie.setYear(year);

        DynamoDBQueryExpression<Movie> queryExpression = new DynamoDBQueryExpression<Movie>()
                .withHashKeyValues(movie)
                .withLimit(10)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withConsistentRead(false);

        return mapper.query(Movie.class, queryExpression);
    }

    public static List<Movie> queryMoviesByYearAndFirstLetter(AmazonDynamoDB amazonDynamoDB, int year, String character) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        Movie movie = new Movie();
        movie.setYear(year);

        DynamoDBQueryExpression<Movie> queryExpression = new DynamoDBQueryExpression<Movie>()
                .withHashKeyValues(movie)
                .withKeyConditionExpression("#title EQUALS :" + character)
                .withLimit(10)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withConsistentRead(false);

        return mapper.query(Movie.class, queryExpression);
    }

    public static Movie loadMovieByYear(AmazonDynamoDB amazonDynamoDB, Movie movieItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);
        return mapper.load(movieItem);
    }

    /**
     * Creates a Global Secondary Index on the Streams Table given a hash-key and optional range-key.
     *
     * @param amazonDynamoDB {@link AmazonDynamoDB}
     * @param hashKey index hash-key value
     * @param rangeKey optional range-key value
     *
     * @return boolean
     */
    public static boolean createGlobalSecondaryIndexOnStreamsTable(AmazonDynamoDB amazonDynamoDB, @NotNull String hashKey, String rangeKey) throws IOException {
        if (hashKey == null) {
            throw new IOException("GSI Hashkey must have an explicit value and can not be null");
        }

        UpdateTableResult updateTableResult;
        try {

            // assign the key schema for the secondary index
            ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<>();
            indexKeySchema.add(new KeySchemaElement().withAttributeName(hashKey).withKeyType(KeyType.HASH));
            if (rangeKey != null) {
                indexKeySchema.add(new KeySchemaElement().withAttributeName(rangeKey).withKeyType(KeyType.RANGE));
            }

            // Add the current attribute definitions in the update
            ArrayList<AttributeDefinition> indexAttributeDefinitions = new ArrayList<>();
            indexAttributeDefinitions.add(new AttributeDefinition().withAttributeName("temperature").withAttributeType(ScalarAttributeType.S));
            indexAttributeDefinitions.add(new AttributeDefinition().withAttributeName("machineId").withAttributeType(ScalarAttributeType.N));
            indexAttributeDefinitions.add(new AttributeDefinition().withAttributeName("machineType").withAttributeType(ScalarAttributeType.S));

            CreateGlobalSecondaryIndexAction globalSecondaryIndex = new CreateGlobalSecondaryIndexAction()
                    .withIndexName("temperature-index")
                    .withKeySchema(indexKeySchema)
                    .withProjection(new Projection().withProjectionType(ProjectionType.INCLUDE).withNonKeyAttributes("machineId", "machineType"))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

            GlobalSecondaryIndexUpdate globalSecondaryIndexUpdate = new GlobalSecondaryIndexUpdate();
            globalSecondaryIndexUpdate.setCreate(globalSecondaryIndex);

            UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                    .withTableName(STREAMS_TABLE)
                    .withGlobalSecondaryIndexUpdates(globalSecondaryIndexUpdate)
                    .withAttributeDefinitions(indexAttributeDefinitions);

            updateTableResult = amazonDynamoDB.updateTable(updateTableRequest);
        } catch (Exception e) {
            System.out.println("Error creating GSI on Streams Table: " + e.getMessage());
            return false;
        }
        System.out.println(updateTableResult.getTableDescription());
        return true;
    }

    /**
     * Provides a method to query the Streams Table temperature-index by supplying a {@link Streams} model 
     * with a set temperature value. 
     * 
     * @param amazonDynamoDB {@link AmazonDynamoDB}
     * @param streamsItem {@link Streams}
     * 
     * @return {@link PaginatedQueryList}
     */
    public static PaginatedQueryList<Streams> queryStreamsTemperatureIndexByTemperature(AmazonDynamoDB amazonDynamoDB, Streams streamsItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        DynamoDBQueryExpression<Streams> queryExpression = new DynamoDBQueryExpression<Streams>()
                .withIndexName("temperature-index")
                .withHashKeyValues(streamsItem)
                .withLimit(10)
                .withConsistentRead(false);

        return mapper.query(Streams.class, queryExpression);
    }

    public static Streams loadStreamsItemByMachineIdAndType(AmazonDynamoDB amazonDynamoDB, Streams streamsItem) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);
        return  mapper.load(streamsItem);
    }
}
