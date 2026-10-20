-- Example target tables for the toy domain. The sink inserts the columns that exist in BOTH the lake table and the target
-- (case-insensitive); columns starting with "_", identity and computed columns are never written.
CREATE TABLE dbo.Customers (
    event_id     varchar(36)  NOT NULL,
    tenant_id    int          NOT NULL,
    customer_id  int          NOT NULL,
    name         nvarchar(200) NULL,
    email        nvarchar(200) NULL,
    country      varchar(2)   NULL,
    created_at   datetime2(3) NULL,
    recorded_at  datetime2(3) NULL,
    deleted      int          NOT NULL,
    seq_val      bigint       NULL,
    operation    int          NOT NULL
);
GO
CREATE TABLE dbo.Orders (
    event_id      varchar(36)  NOT NULL,
    tenant_id     int          NOT NULL,
    order_id      int          NOT NULL,
    customer_id   int          NOT NULL,
    status        varchar(20)  NULL,
    total_amount  decimal(18,2) NULL,
    ordered_at    datetime2(3) NULL,
    recorded_at   datetime2(3) NULL,
    deleted       int          NOT NULL,
    seq_val       bigint       NULL,
    operation     int          NOT NULL
);
GO
CREATE TABLE dbo.CustomersSnapshot (
    tenant_id    int          NOT NULL,
    customer_id  int          NOT NULL,
    name         nvarchar(200) NULL,
    email        nvarchar(200) NULL,
    country      varchar(2)   NULL,
    created_at   datetime2(3) NULL
);
