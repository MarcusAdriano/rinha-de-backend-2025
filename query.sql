-- name: GetSummaryApiUsage :many
SELECT api, sum(amount), count(api) AS count
FROM payments
WHERE requested_at >= @pfrom::timestamp AND requested_at < @pto::timestamp
GROUP BY api;

-- name: InsertPayment :exec
INSERT INTO payments (correlationId, amount, requested_at, api)
VALUES ($1, $2, $3, $4);

-- name: PurgePayments :exec
TRUNCATE TABLE payments;