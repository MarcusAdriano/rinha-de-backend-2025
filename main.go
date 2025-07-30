package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type PaymentRequest struct {
	CorrelationId *string  `json:"correlationId,omitempty"`
	Amount        *float32 `json:"amount,omitempty"`
}

func main() {
	go func() {
		http.HandleFunc("POST /payments", paymentsHandler)
		http.HandleFunc("GET /payments-summary", paymentsSummaryHandler)
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	for {
		select {
		case request := <-mainQueue:
			log.Printf("Processing payment request: %+v", request)
			// Here you would process the payment request
		}
	}
}

var mainQueue = make(chan PaymentRequest, 500)

func paymentsHandler(w http.ResponseWriter, r *http.Request) {
	var request PaymentRequest

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&request)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	mainQueue <- request

	w.WriteHeader(http.StatusAccepted)
}

func paymentsSummaryHandler(w http.ResponseWriter, r *http.Request) {

}
