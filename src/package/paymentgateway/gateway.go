package paymentgateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	defaultRinhaBackendToken = "123"
	rinhaTokenHeaderName     = "x-rinha-token"
)

var (
	ErrHttpIsNot200OK  = errors.New("payment paymentgateway return status code different than 200 OK")
	ErrPaymentNotFound = errors.New("payment not found")
)

type PaymentParams struct {
	CorrelationID uuid.UUID `json:"correlationId,omitempty"`
	Amount        float64   `json:"amount,omitempty"`
	RequestedAt   time.Time `json:"requestedAt,omitempty"`
}

type PaymentSearchResponse struct {
	CorrelationID string  `json:"correlationId,omitempty"`
	Amount        float64 `json:"amount,omitempty"`
	RequestedAt   string  `json:"requestedAt,omitempty"`
}

type HealthCheckResponse struct {
	Failing         bool  `json:"failing,omitempty"`
	MinResponseTime int32 `json:"min_response_time,omitempty"`
}

type PaymentGateway interface {
	Process(payment PaymentParams) error
	HealthCheck() (*HealthCheckResponse, error)
	PurgePayments() error
	GetPaymentById(id string) (PaymentSearchResponse, error)
}

type paymentGatewayImpl struct {
	baseUrl    string
	httpClient *http.Client
}

func NewGateway(host string, client *http.Client) PaymentGateway {
	return &paymentGatewayImpl{
		baseUrl:    host,
		httpClient: client,
	}
}

func (g *paymentGatewayImpl) Process(payment PaymentParams) error {
	url := g.baseUrl + "/payments"
	method := "POST"

	encodeRequest, err := json.Marshal(payment)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(method, url, bytes.NewReader(encodeRequest))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := g.httpClient.Do(req)
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

func IsHttpError(err error) bool {
	if errors.Is(err, ErrHttpIsNot200OK) {
		return true
	}
	return false
}

func (g *paymentGatewayImpl) HealthCheck() (*HealthCheckResponse, error) {
	url := g.baseUrl + "/payments/service-health"
	method := "GET"

	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, err
	}
	res, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, errors.New(res.Status)
	}

	var healthCheckResponse HealthCheckResponse
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&healthCheckResponse); err != nil {
		return nil, err
	}

	return &healthCheckResponse, nil
}

func (g *paymentGatewayImpl) GetPaymentById(id string) (PaymentSearchResponse, error) {
	url := g.baseUrl + "/payments/" + id
	method := "GET"

	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return PaymentSearchResponse{}, err
	}

	res, err := g.httpClient.Do(req)
	if err != nil {
		return PaymentSearchResponse{}, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return PaymentSearchResponse{}, ErrPaymentNotFound
	} else if res.StatusCode != http.StatusOK {
		return PaymentSearchResponse{}, ErrHttpIsNot200OK
	}

	var payment PaymentSearchResponse
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&payment); err != nil {
		return PaymentSearchResponse{}, err
	}

	return payment, nil
}

func (g *paymentGatewayImpl) PurgePayments() error {
	url := g.baseUrl + "/admin/purge-payments"
	method := "POST"

	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		log.Println(err)
		return err
	}
	req.Header.Add(rinhaTokenHeaderName, defaultRinhaBackendToken)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Printf("%s -> %s\n", url, string(body))
	return nil
}
