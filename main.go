package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"io/ioutil"
	"log"
	"net/http"
	"rinha-de-backend-2025/dbpayments"
	"strconv"
	"time"
)

const (
	ApiMain     = 0
	ApiFallback = 1
)

type PaymentRequest struct {
	CorrelationId string  `json:"correlationId,omitempty"`
	Amount        float64 `json:"amount,omitempty"`
	RequestedDate time.Time
	Attempts      *int
}

var queries *dbpayments.Queries

func main() {

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "host=localhost user=postgres dbname=dbpayments password=postgres")
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", err)
		return
	}
	defer conn.Close(ctx)
	queries = dbpayments.New(conn)

	go func() {
		http.HandleFunc("POST /payments", paymentsHandler)
		http.HandleFunc("GET /payments-summary", paymentsSummaryHandler)
		log.Fatal(http.ListenAndServe(":9999", nil))
	}()

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
	Timeout: 200 * time.Millisecond,
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

	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func makePaymentMain(request *PaymentRequest) {
	err := makePayment("http://localhost:8001", request)
	if err != nil {
		if *request.Attempts > 3 {
			fallbackQueue <- request
			return
		}
		*request.Attempts++
		mainQueue <- request
		return
	}

	insertPaymentToDB(ApiMain, request)
}

func makePaymentFallback(request *PaymentRequest) {
	err := makePayment("http://localhost:8002", request)
	if err != nil {
		log.Printf("Failed to process payment after fallback: %v", err)
		return
	}

	insertPaymentToDB(ApiFallback, request)
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
		RequestedAt:   request.RequestedDate,
		Api:           apiVersion,
	})
}

var mainQueue = make(chan *PaymentRequest, 500_000)
var fallbackQueue = make(chan *PaymentRequest, 500_000)

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

	if request.CorrelationId == "" {
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

	summary, err := queries.GetSummaryApiUsage(context.Background(), dbpayments.GetSummaryApiUsageParams{
		Pfrom: fromTime,
		Pto:   toTime,
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
