package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"rinha-de-backend-2025/src/internal/model"
)

type Gateway interface {
	process(payment model.PaymentParams) error
}

type GatewayImpl struct {
	baseUrl    string
	httpClient *http.Client
}

func NewGateway(host string, client *http.Client) Gateway {
	return &GatewayImpl{
		baseUrl:    host,
		httpClient: client,
	}
}

func (g *GatewayImpl) process(payment model.PaymentParams) error {
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
