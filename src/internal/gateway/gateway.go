package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"rinha-de-backend-2025/src/internal/model"
)

type HealthCheckResponse struct {
	Failing         bool  `json:"failing,omitempty"`
	MinResponseTime int32 `json:"min_response_time,omitempty"`
}

type PaymentGateway interface {
	Process(payment model.PaymentParams) error
	HealthCheck() (*HealthCheckResponse, error)
}

type PaymentGatewayImpl struct {
	baseUrl    string
	httpClient *http.Client
}

func NewGateway(host string, client *http.Client) PaymentGateway {
	return &PaymentGatewayImpl{
		baseUrl:    host,
		httpClient: client,
	}
}

func (g *PaymentGatewayImpl) Process(payment model.PaymentParams) error {
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
	if errors.Is(err, errHealthCheckHttpNotOk) {
		return true
	}
	return false
}

var errHealthCheckHttpNotOk = errors.New("payment gateway return status code different than 200 OK")

func (g *PaymentGatewayImpl) HealthCheck() (*HealthCheckResponse, error) {
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
