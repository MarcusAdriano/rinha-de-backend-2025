package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log"
	"net/http"
	"os"
	"rinha-de-backend-2025/dbpayments"
	"strconv"
	"time"
)

const (
	ApiMain     = 0
	ApiFallback = 1
)

var (
	MainProcessor     = "http://localhost:8001"
	FallbackProcessor = "http://localhost:8002"
	DatabaseUrl       = "postgres://postgres:postgres@localhost:5432/dbpayments?pool_max_conns=6&pool_min_conns=6&pool_max_conn_lifetime=330s"
	mainQueue         = make(chan *PaymentRequest, 750_000)
	fallbackQueue     = make(chan *PaymentRequest, 750_000)
	queries           *dbpayments.Queries
)

type PaymentRequest struct {
	CorrelationId string  `json:"correlationId,omitempty"`
	Amount        float64 `json:"amount,omitempty"`
	RequestedDate time.Time
	Attempts      *int
}

func createDatabaseConnection(connectionString string) *dbpayments.Queries {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		log.Printf("Error parsing connection string: %+v", err)
		panic(err)
	}

	hostname, _ := os.Hostname()
	appName := "rinha-2025-" + hostname

	config.ConnConfig.Config.ConnectTimeout = time.Second * 1
	config.ConnConfig.RuntimeParams["application_name"] = appName
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Printf("Error connecting to database: %+v", err)
		panic(err)
	}

	return dbpayments.New(pool)
}

func main() {

	mainProcessorUrl := os.Getenv("MAIN_PROCESSOR_URL")
	if mainProcessorUrl != "" {
		MainProcessor = mainProcessorUrl
		log.Printf("Using main processor URL: %s\n", MainProcessor)
	} else {
		log.Printf("MAIN_PROCESSOR_URL is NULL: Using default main processor URL: %s\n", MainProcessor)
	}

	fallbackProcessorUrl := os.Getenv("FALLBACK_PROCESSOR_URL")
	if fallbackProcessorUrl != "" {
		FallbackProcessor = fallbackProcessorUrl
		log.Printf("Using fallback processor URL: %s\n", FallbackProcessor)
	} else {
		log.Printf("FALLBACK_PROCESSOR_URL is NULL: Using default fallback processor URL: %s\n", FallbackProcessor)
	}

	connectionString := os.Getenv("DATABASE_URL")
	if connectionString != "" {
		DatabaseUrl = connectionString
		log.Printf("Using database URL: %s\n", DatabaseUrl)
	} else {
		log.Printf("DATABASE_URL is NULL: Using default database URL: %s\n", DatabaseUrl)
	}

	queries = createDatabaseConnection(DatabaseUrl)

	go func() {
		http.HandleFunc("POST /payments", paymentsHandler)
		http.HandleFunc("GET /payments-summary", paymentsSummaryHandler)
		http.HandleFunc("POST /purge-payments", purgeDatabaseHandler)
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	log.Printf("Running payment processor...\n")

	for {
		select {
		case request := <-mainQueue:
			request.RequestedDate = time.Now()
			makePaymentMain(request)

		case request := <-fallbackQueue:
			request.RequestedDate = time.Now()
			makePaymentFallback(request)
		}
	}
}

var httpClient = &http.Client{
	Timeout: 100 * time.Millisecond,
}

func makePayment(baseUrl string, request *PaymentRequest) error {
	url := baseUrl + "/payments"
	method := "POST"

	encodeRequest, err := json.Marshal(request)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(method, url, bytes.NewReader(encodeRequest))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return errors.New(res.Status)
	}

	_, err = io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func makePaymentMain(request *PaymentRequest) {
	err := makePayment(MainProcessor, request)
	if err != nil {
		//log.Printf("[main processor] Unable to make payment: %v\n", err)
		if *request.Attempts > 3 {
			go func() {
				time.Sleep(time.Duration(*request.Attempts) * time.Second)
				fallbackQueue <- request
			}()
			return
		}
		*request.Attempts++
		go func() {
			time.Sleep(time.Duration(*request.Attempts) * time.Second)
			mainQueue <- request
		}()
		return
	}

	err = insertPaymentToDB(ApiMain, request)
	if err != nil {
		log.Printf("[main processor] Unable to insert payment: %v\n", err)
	}
}

func makePaymentFallback(request *PaymentRequest) {
	err := makePayment(FallbackProcessor, request)
	if err != nil {
		//log.Printf("[fallback processor] Unable to make payment: %v", err)
		return
	}

	err = insertPaymentToDB(ApiFallback, request)
	if err != nil {
		log.Printf("[fallback processor] Unable to insert payment: %v\n", err)
	}
}

func insertPaymentToDB(apiVersion int32, request *PaymentRequest) error {
	id, _ := uuid.Parse(request.CorrelationId)

	stringValue := strconv.FormatFloat(request.Amount, 'f', -1, 64)
	var amount pgtype.Numeric
	err := amount.Scan(stringValue)
	if err != nil {
		log.Printf("Failed to scan amount: %v", err)
		return err
	}

	return queries.InsertPayment(context.Background(), dbpayments.InsertPaymentParams{
		Correlationid: id,
		Amount:        amount,
		RequestedAt:   request.RequestedDate.UTC(),
		Api:           apiVersion,
	})
}

func paymentsHandler(w http.ResponseWriter, r *http.Request) {
	var request PaymentRequest

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&request)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.Amount <= 0 {
		http.Error(w, "Invalid amount", http.StatusUnprocessableEntity)
		return
	}

	_, err = uuid.Parse(request.CorrelationId)
	if request.CorrelationId == "" || err != nil {
		http.Error(w, "Invalid correlation id", http.StatusUnprocessableEntity)
		return
	}

	att := 1
	request.Attempts = &att

	mainQueue <- &request

	w.WriteHeader(http.StatusOK)
}

type summaryRow struct {
	Amount   float64 `json:"totalAmount"`
	Requests int64   `json:"totalRequests"`
}

func paymentsSummaryHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	query := r.URL.Query()
	from := query.Get("from")
	to := query.Get("to")

	if from == "" || to == "" {
		http.Error(w, "Missing 'from' or 'to' query parameters", http.StatusUnprocessableEntity)
		return
	}

	fromTime, err := time.Parse(time.RFC3339, from)
	if err != nil {
		http.Error(w, "Invalid 'from' date format", http.StatusUnprocessableEntity)
		return
	}

	toTime, err := time.Parse(time.RFC3339, to)
	if err != nil {
		http.Error(w, "Invalid 'to' date format", http.StatusUnprocessableEntity)
		return
	}

	log.Printf("Fetching payments summary from: %v, to: %v\n", fromTime, toTime)

	summary, err := queries.GetSummaryApiUsage(context.Background(), dbpayments.GetSummaryApiUsageParams{
		Pfrom: fromTime.UTC(),
		Pto:   toTime.UTC(),
	})
	if err != nil {
		http.Error(w, "Error fetching summary", http.StatusInternalServerError)
		return
	}

	response := make(map[string]interface{})
	defaultt := summaryRow{
		Amount:   0,
		Requests: 0,
	}
	fallback := summaryRow{
		Amount:   0,
		Requests: 0,
	}

	for _, row := range summary {
		val, _ := row.Sum.Float64Value()

		if row.Api == 0 {
			defaultt.Requests = row.Count
			defaultt.Amount = val.Float64
		} else {
			fallback.Requests = row.Count
			fallback.Amount = val.Float64
		}
	}
	response["default"] = defaultt
	response["fallback"] = fallback

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Error marshalling response", http.StatusInternalServerError)
		return
	}
	w.Write(jsonResponse)
}

func purgeDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	err := queries.PurgePayments(context.TODO())
	if err != nil {
		log.Printf("Error purging database: %v\n", err)
	}

	purgePaymentProcessors(MainProcessor)
	purgePaymentProcessors(FallbackProcessor)
}

func purgePaymentProcessors(host string) {
	url := host + "/admin/purge-payments"
	method := "POST"

	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("x-rinha-token", "123")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Printf("%s -> %s\n", url, string(body))
}
