package datalayer

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/ecsdderekwicks/flights/graph/model"
	"github.com/google/uuid"
)

func initialiseDb() *dynamodb.DynamoDB {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	return dynamodb.New(sess)
}

// In order to reuse the connection, the best practice is to set up a struct
// that will hold the data about the database.
type Database struct {
	svc *dynamodb.DynamoDB
}

// Create new Database struct
func NewDatabase() Database {
	d := Database{}
	dynamo := initialiseDb()
	d.svc = dynamo
	return d
}

func (d *Database) scanTable(tableName string) (*dynamodb.ScanOutput, error) {
	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	// Make the DynamoDB Query API call
	return d.svc.Scan(params)
}

func (d *Database) CreatePassenger(name string) (*model.Passenger, error) {
	item := model.Passenger{
		ID:   uuid.New().String(),
		Name: name,
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Printf("Got error marshalling new passenger item: %v\n", err.Error())
		return nil, err
	}

	// Create item in table passengers
	tableName := "passengers"

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	// TODO change here
	_, err = d.svc.PutItem(input)
	if err != nil {
		fmt.Printf("Got error calling PutItem: %v\n", err.Error())
		return nil, err
	}

	return &item, nil
}

func (d *Database) DeletePassenger(passengerId string) (bool, error) {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(passengerId),
			},
		},
		TableName: aws.String("passengers"),
	}

	_, err := d.svc.DeleteItem(input)
	if err != nil {
		fmt.Printf("Got error calling DeleteItem: %v\n", err.Error())
		return false, err
	}

	return true, nil
}

// Adds "setItem" to the StringSet (SS) identified by "setAttribute" on the record with a
// a partition key of "keyAttribute" with the value of "key" in the Dynamo table "table".
func addToSet(db *dynamodb.DynamoDB, table, keyAttribute, key, setAttribute, setItem string) error {
	_, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#0": &setAttribute,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":0": {SS: []*string{&setItem}},
		},
		Key: map[string]*dynamodb.AttributeValue{
			keyAttribute: {S: &key},
		},
		TableName:        &table,
		UpdateExpression: aws.String("ADD #0 :0"),
	})
	return err
}

// Deletes "setItem" from the StringSet (SS) identified by "setAttribute" on the record with a
// a partition key of "keyAttribute" with the value of "key" in the Dynamo table "table".
func deleteFromSet(db *dynamodb.DynamoDB, table, keyAttribute, key, setAttribute, setItem string) error {
	_, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#0": &setAttribute,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":0": {SS: []*string{&setItem}},
		},
		Key: map[string]*dynamodb.AttributeValue{
			keyAttribute: {S: &key},
		},
		TableName:        &table,
		UpdateExpression: aws.String("DELETE #0 :0"),
	})
	return err
}

func (d *Database) BookFlight(flightNumber string, passengerId string) (bool, error) {
	err := addToSet(d.svc, "flights", "number", flightNumber, "passengers", passengerId)

	if err != nil {
		fmt.Println(err.Error())
		return false, err
	}

	return true, nil
}

func (d *Database) CancelBooking(flightNumber string, passengerId string) (bool, error) {
	err := deleteFromSet(d.svc, "flights", "number", flightNumber, "passengers", passengerId)

	if err != nil {
		fmt.Println(err.Error())
		return false, err
	}

	return true, nil
}

type DynamoFlight struct {
	Number     string
	Passengers []string
	Capacity   int
	Captain    string
	Plane      string
}

func (d *Database) GetAllFlights() ([]*model.Flight, error) {
	result, err := d.scanTable("flights")

	if err != nil {
		fmt.Printf("Query API call failed: %v\n", err.Error())
		return nil, err
	}

	var flights []*model.Flight

	for _, dynamoItem := range result.Items {
		item := DynamoFlight{}

		err = dynamodbattribute.UnmarshalMap(dynamoItem, &item)

		if err != nil {
			fmt.Printf("Got error unmarshalling: %v\n", err.Error())
			return nil, err
		}

		flight, err := convertDynamoFlightToFlight(item)

		if err != nil {
			return nil, err
		}

		flights = append(flights, flight)
	}

	return flights, nil
}

func convertDynamoFlightToFlight(dynamoFlight DynamoFlight) (*model.Flight, error) {
	flight := model.Flight{
		Number:   dynamoFlight.Number,
		Capacity: dynamoFlight.Capacity,
		Captain:  dynamoFlight.Captain,
		Plane:    dynamoFlight.Plane,
	}

	for _, passengerId := range dynamoFlight.Passengers {
		passenger, err := GetPassenger(passengerId)

		if err != nil {
			fmt.Printf("Query API call failed: %v\n", err.Error())
			return nil, err
		}

		flight.Passengers = append(flight.Passengers, passenger)
	}

	return &flight, nil
}

func (d *Database) GetPassenger(passengerId string) (*model.Passenger, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(passengerId),
			},
		},
		TableName: aws.String("passengers"),
	}

	dynamoItem, err := d.svc.GetItem(input)

	if err != nil {
		fmt.Printf("Query API call failed: %v\n", err.Error())
		return nil, err
	}

	item := model.Passenger{}

	err = dynamodbattribute.UnmarshalMap(dynamoItem.Item, &item)

	if err != nil {
		fmt.Printf("Got error unmarshalling: %v\n", err.Error())
		return nil, err
	}

	return &item, nil
}

func (d *Database) GetAllPassengers() ([]*model.Passenger, error) {
	result, err := d.scanTable("passengers")

	if err != nil {
		fmt.Printf("Query API call failed: %v\n", err.Error())
		return nil, err
	}

	var passengers []*model.Passenger

	for _, dynamoItem := range result.Items {
		item := model.Passenger{}

		err = dynamodbattribute.UnmarshalMap(dynamoItem, &item)

		if err != nil {
			fmt.Printf("Got error unmarshalling: %v\n", err.Error())
			return nil, err
		}

		passengers = append(passengers, &item)
	}

	return passengers, nil
}
