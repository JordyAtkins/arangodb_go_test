package main

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"log"
	"time"
)

const ConnectionString = "http://localhost:8529"

// Flight structure matching that in arangoDB
type Airport struct {
	Airport string  `json:"airport"`
	City    string  `json:"city"`
	State   string  `json:"state"`
	Country string  `json:"country"`
	Lat     float64 `json:"lat"`
	Long    float64 `json:"long"`
}

// Flight structure matching that in arangoDB
type Flight struct {
	Year          int       `json:"Year"`
	Month         int       `json:"Month"`
	DayofMonth    int       `json:"DayofMonth"`
	DayOfWeek     int       `json:"DayOfWeek"`
	DepTime       int       `json:"DepTime"`
	ArrTime       int       `json:"ArrTime"`
	DepTimeUTC    time.Time `json:"DepTimeUTC"`
	ArrTimeUTC    time.Time `json:"ArrTimeUTC"`
	UniqueCarrier string    `json:"UniqueCarrier"`
	FlightNum     int       `json:"FlightNum"`
	TailNum       string    `json:"TailNum"`
	Distance      int       `json:"Distance"`
}

// Wrapper around DocumentMeta to allow for the Printable interface to be used
type MetaInfo driver.DocumentMeta

// Interface to easily print out contents
type Printable interface {
	Print()
}

func main() {
	t := time.Now()
	conn := getConnection()
	c := getClient(conn)
	db := getDatabase(c, "")

	// Testing single entity retrial
	printAirportUsingKey(db, "M75")
	printFlightUsingKey(db, "350814")

	// Simple Airport queries
	getFirstNAirports(db, 0)
	getFirstNAirports(db, 10)
	getFirstNAirports(db, 100)

	// Simple Flight queries
	getFirstNFlights(db, 0)
	getFirstNFlights(db, 10)
	getFirstNFlights(db, 100)

	fmt.Println("Total time taken :", time.Now().Sub(t))
}

// Gets the first N airports from the "airports" collection
// If n is less than or equal to 0 then it is defaulted to 20
func getFirstNAirports(db driver.Database, n int) {
	if n <= 0 {
		n = 20
	}
	aql := `
FOR a IN airports
LIMIT @n
RETURN a`

	res, err := db.Query(context.Background(), aql, map[string]interface{}{"n": n})

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	defer res.Close()

	for res.HasMore() {
		var airports Airport
		meta, err := res.ReadDocument(context.Background(), &airports)

		if err != nil {
			log.Fatal(err)
		}

		printContents(MetaInfo(meta), airports)
		fmt.Println("------------")
	}
	fmt.Println(time.Now().Sub(t))

}

// Gets the first N flights from the "flights" collection
// If n is less than or equal to 0 then it is defaulted to 20
func getFirstNFlights(db driver.Database, n int) {
	if n <= 0 {
		n = 20
	}
	aql := `
FOR f IN flights
LIMIT @n
RETURN f`

	res, err := db.Query(context.Background(), aql, map[string]interface{}{"n": n})

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	defer res.Close()

	for res.HasMore() {
		var flight Flight
		meta, err := res.ReadDocument(context.Background(), &flight)

		if err != nil {
			log.Fatal(err)
		}

		printContents(MetaInfo(meta), flight)
		fmt.Println("------------")
	}
	fmt.Println(time.Now().Sub(t))

}

// Prints the contents of an airport found in the arangoDB collection "flights" with the matching key
func printFlightUsingKey(db driver.Database, key string) {
	flights, err := db.Collection(context.Background(), "flights")
	if err != nil {
		log.Fatal(err)
	}
	var matchingFlight Flight
	meta, err := flights.ReadDocument(nil, key, &matchingFlight)
	if err != nil {

	}

	printContents(MetaInfo(meta), matchingFlight)
}

// Prints the contents of an airport found in the arangoDB collection "airports" with the matching key
func printAirportUsingKey(db driver.Database, key string) {
	airports, err := db.Collection(context.Background(), "airports")
	if err != nil {
		log.Fatal(err)
	}
	var firstAirport Airport
	meta, err := airports.ReadDocument(nil, key, &firstAirport)
	if err != nil {
		log.Fatal(err)
	}

	printContents(MetaInfo(meta), firstAirport)
}

// Gets the database by name. If the name is not provided then _system is used
func getDatabase(c driver.Client, name string) driver.Database {
	// Override the name if nothing is provided
	if name == "" {
		name = "_system"
	}

	res, err := c.Database(nil, name)
	if err != nil {
		log.Fatal(err)
	}
	return res
}

// Gets the connection for the arangoDB
func getConnection() driver.Connection {
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{ConnectionString},
	})
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

// Prints out the instance content of a Flight
func (a Airport) Print() {
	fmt.Println(a.Airport)
	fmt.Println(a.City)
	fmt.Println(a.Country)
	fmt.Println(a.Lat)
	fmt.Println(a.Long)
	fmt.Println(a.State)
}

// Prints out the instance content of a Flight
func (f Flight) Print() {
	fmt.Println(f.ArrTime)
	fmt.Println(f.ArrTimeUTC)
	fmt.Println(f.DayofMonth)
	fmt.Println(f.DayOfWeek)
	fmt.Println(f.DepTime)
	fmt.Println(f.Distance)
	fmt.Println(f.FlightNum)
	fmt.Println(f.Month)
	fmt.Println(f.TailNum)
	fmt.Println(f.UniqueCarrier)
	fmt.Println(f.Year)
}

// Prints out the metadata information using fmt.Println
// Here as a helper method to remove duplication
func (meta MetaInfo) Print() {
	fmt.Println(meta.ID)
	fmt.Println(meta.Rev)
	fmt.Println(meta.Key)
}

// Gets the arangoDB client
func getClient(conn driver.Connection) driver.Client {
	c, err := driver.NewClient(getConfiguration(conn))
	if err != nil {
		log.Fatal(err)
	}
	return c
}

// Gets the configuration for connecting to the arangoDB instance
func getConfiguration(conn driver.Connection) driver.ClientConfig {
	return driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication("root", ""), // Update this at some point
	}
}

func printContents(printable ...Printable) {
	for _, p := range printable {
		p.Print()
	}
}
