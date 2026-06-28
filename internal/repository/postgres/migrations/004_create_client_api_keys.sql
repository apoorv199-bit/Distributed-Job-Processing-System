-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS client_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id VARCHAR(255) NOT NULL UNIQUE,
    key_prefix VARCHAR(16) NOT NULL,
    hashed_key VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active' NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_client_api_keys_hashed_key ON client_api_keys(hashed_key);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS client_api_keys;
-- +goose StatementEnd
