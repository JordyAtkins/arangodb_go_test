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
	FromAirport   string    `json:"_from"`
	ToAirport     string    `json:"_to"`
}

// Wrapper around DocumentMeta to allow for the Printable interface to be used
type MetaInfo driver.DocumentMeta

// Interface to easily print out contents
type Printable interface {
	Print()
}

func main() {
	defer timeTaken(time.Now(), "Total Elapsed Time")

	conn := getConnection()
	c := getClient(conn)
	db := getDatabase(c, "")

	// Testing single entity retrial
	printAirportUsingKey(db, "M75")
	printFlightUsingKey(db, "350814")

	// Simple Airport queries
	printAirports(db, 0)
	printAirports(db, 10)
	printAirports(db, 100)

	// Simple Flight queries
	printFlights(db, 0)
	printFlights(db, 10)
	printFlights(db, 100)

	res := getAirportCountPerState(db)

	for k, v := range res {
		fmt.Println(k)
		fmt.Println(v)
	}

	printFlightsFromAirportCode(db, 20, "LAX")

	createNewAirport(db)
}

// Prints the first N airports from the "airports" collection
// If n is less than or equal to 0 then it is defaulted to 20
func printAirports(db driver.Database, n int) {
	if n <= 0 {
		n = 20
	}
	query := `
FOR a IN airports
LIMIT @n
RETURN a`

	res, err := db.Query(context.Background(), query, map[string]interface{}{"n": n})

	if err != nil {
		log.Fatal(err)
	}

	defer timeTaken(time.Now(), "getFirstNAirports")
	defer res.Close()

	for res.HasMore() {
		var airports Airport
		meta, err := res.ReadDocument(context.Background(), &airports)

		if err != nil {
			log.Fatal(err)
		}

		printContents(MetaInfo(meta), airports)
	}

}

// Prints the first N flights from the "flights" collection
// If n is less than or equal to 0 then it is defaulted to 20
func printFlights(db driver.Database, n int) {
	if n <= 0 {
		n = 20
	}
	query := `
FOR f IN flights
LIMIT @n
RETURN f`

	res, err := db.Query(context.Background(), query, map[string]interface{}{"n": n})

	if err != nil {
		log.Fatal(err)
	}

	defer timeTaken(time.Now(), "getFirstNFlights")
	defer res.Close()

	for res.HasMore() {
		var flight Flight
		meta, err := res.ReadDocument(context.Background(), &flight)

		if err != nil {
			log.Fatal(err)
		}

		printContents(MetaInfo(meta), flight)
	}
}

// Prints the different states and the number of airports within that state
func getAirportCountPerState(db driver.Database) map[string]float64 {
	query := `
FOR a IN airports
COLLECT state = a.state
WITH COUNT INTO counter
RETURN {state, counter}
`
	res, err := db.Query(context.Background(), query, nil)

	if err != nil {
		log.Fatal(err)
	}
	defer timeTaken(time.Now(), "getAirportCountPerState")
	defer res.Close()

	retVal := map[string]float64{}

	for res.HasMore() {
		var queryResult struct {
			State   string  `json:"state"`
			Counter float64 `json:"counter"`
		}

		_, err := res.ReadDocument(context.Background(), &queryResult)
		if err != nil {
			log.Fatal(err)
		}

		retVal[queryResult.State] = queryResult.Counter
	}

	return retVal
}

func createNewAirport(db driver.Database) {
	newAirport := Airport{
		Airport: "A new one",
		State:   "NA",
		Lat:     39.5155436,
		Long:    -84.29460752,
		Country: "USA",
		City:    "Cincinnati",
	}

	col, err := db.Collection(context.Background(), "airports")

	if err != nil {
		log.Fatal(err)
	}

	meta, err := col.CreateDocument(context.Background(), newAirport)

	if err != nil {
		log.Fatal(err)
	}

	printContents(MetaInfo(meta))

	printAirportUsingKey(db, meta.Key)
}

// Print the flights from the supplied departure airport code
func printFlightsFromAirportCode(db driver.Database, n int, code string) {
	airportCode := fmt.Sprintf("airports/%s", code)
	query := `
FOR a, f IN OUTBOUND @airportCode flights
LIMIT @count
RETURN {a,f}
`
	res, err := db.Query(context.Background(), query, map[string]interface{}{"airportCode": airportCode, "count": n})
	if err != nil {
		log.Fatal(err)
	}

	defer timeTaken(time.Now(), "getNFlightsFromAirport")
	defer res.Close()

	for res.HasMore() {
		var queryResult struct {
			Airport `json:"a"`
			Flight  `json:"f"`
		}
		_, err := res.ReadDocument(context.Background(), &queryResult)
		if err != nil {
			log.Fatal(err)
		}
		printContents(queryResult.Airport, queryResult.Flight)
	}
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

// Prints out the instance content of a Airport
func (a Airport) Print() {
	fmt.Println(a.Airport, a.City, a.Country, a.Lat, a.Long, a.State)
}

// Prints out the instance content of a Flight
func (f Flight) Print() {
	fmt.Println(f.ArrTime, f.ArrTimeUTC, f.DayofMonth, f.DayOfWeek, f.DepTime, f.Distance, f.FlightNum, f.Month, f.TailNum, f.UniqueCarrier, f.Year, f.FromAirport, f.ToAirport)
}

// Prints out the metadata information using fmt.Println
// Here as a helper method to remove duplication
func (meta MetaInfo) Print() {
	fmt.Println(meta.ID, meta.Rev, meta.Key)
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

func timeTaken(start time.Time, msg string) {
	fmt.Println(msg, time.Since(start))
}
