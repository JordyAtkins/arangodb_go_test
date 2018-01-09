package main

import (
	"fmt"
	"github.com/arangodb/go-driver/http"
	"github.com/arangodb/go-driver"
	"log"
	"time"
)

const ConnectionString = "http://localhost:8529"

type Airport struct {
	Airport string  `json:"airport"`
	City    string  `json:"city"`
	State   string  `json:"state"`
	Country string  `json:"country"`
	Lat     float64 `json:"lat"`
	Long    float64 `json:"long"`
}

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

func main() {
	fmt.Println("Hello")
	conn := getConnection()
	c := getClient(conn)
	db := getDatabase(c, "")

	printAirportUsingKey(db)
}

// Prints the contents of an airport found in the arangoDB collection "airports" with the matching key
func printAirportUsingKey(db driver.Database) {
	airports, err := db.Collection(nil, "airports")
	if err != nil {
		log.Fatal(err)
	}
	var firstAirport Airport
	meta, err := airports.ReadDocument(nil, "M75", &firstAirport)
	if err != nil {
		log.Fatal(err)
	}
	printMeta(meta)
	firstAirport.Print()
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
func printMeta(meta driver.DocumentMeta) {
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
