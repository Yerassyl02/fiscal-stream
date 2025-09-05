DROP DATABASE IF EXISTS analytics;
CREATE DATABASE analytics;

CREATE TABLE IF NOT EXISTS analytics.fiscal_clean (
    upload_time                 DateTime,
    company_name                String,
    tax_payer_iin               String,
    kkt_factor_number           String,
    kkt_registration_number     UInt64,
    receipt_number              UInt64,
    date_time                   DateTime,
    category                    String,
    product_name                String,
    unit_price                  Decimal64(2),
    quantity                    UInt64,
    total_amount_by_position    Decimal64(2),
    total_sum                   Decimal64(2),
    vat_rate                    String,
    vat_sum                     Decimal64(2),
    payment                     String,
    fiscal_sign                 UInt64,
    ofd_name                    String,
    ofd_site                    String,
    address                     String,
    qr_code                     String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date_time)
ORDER BY (date_time, tax_payer_iin, upload_time);