package main

import (
	"context"
	"log"
	"time"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/golang/protobuf/proto"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"

	"github.com/amsokol/mongo-go-driver-protobuf"
)

func main() {
	httpClient := &http.Client{}
	req, _ := http.NewRequest("GET", "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/sydneytrains", nil)

	if transportAPIKey, exists := os.LookupEnv("TRANSPORT_API_KEY"); exists {
		req.Header.Add("Authorization", "apikey " + transportAPIKey)
	} else {
		err := "Must set environment variable TRANSPORT_API_KEY"
		log.Fatalln(err)
		panic(err)
	}

	feedMessage := &FeedMessage{}
	if response, err := httpClient.Do(req); err != nil {
		panic(err)
	} else {
		data, _ := ioutil.ReadAll(response.Body)
		if err := proto.Unmarshal(data, feedMessage); err != nil {
			log.Fatalln("Failed to parse FeedMessage:", err)
		}
	}

	log.Printf("connecting to MongoDB...")

	// Register custom codecs for protobuf Timestamp and wrapper types
	reg := codecs.Register(bson.NewRegistryBuilder()).Build()

	// Create MongoDB client with registered custom codecs for protobuf Timestamp and wrapper types
	// NOTE: "mongodb+srv" protocol means connect to Altas cloud MongoDB server
	//       use just "mongodb" if you connect to on-premise MongoDB server
	mongoDbURL, exists := os.LookupEnv("MONGO_DB_URL")

	if !exists {
		err := "Must set environment variable MONGO_DB_URL"
		log.Fatalln(err)
		panic(err)
	}

	client, err := mongo.NewClientWithOptions(mongoDbURL,
		&options.ClientOptions{
			Registry: reg,
		})

	if err != nil {
		log.Fatalf("failed to create new MongoDB client: %#v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect client
	if err = client.Connect(ctx); err != nil {
		log.Fatalf("failed to connect to MongoDB: %#v", err)
	}

	log.Printf("connected successfully")

	// Get collection from database
	coll := client.Database("sydneytrains").Collection("vehiclepos")

	log.Printf("insert data into collection <sydneytrains.vehiclepos>...")

	// Insert data into the collection
	for _, element := range feedMessage.GetEntity() {
		res, err := coll.InsertOne(ctx, &element)
		if err != nil {
			log.Fatalf("insert data into collection <sydneytrains.vehiclepos>: %#v", err)
		}
		id := res.InsertedID
		log.Printf("inserted new item with id=%v successfully", id)
	}
}
