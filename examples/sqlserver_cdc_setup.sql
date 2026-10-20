-- Enable CDC on the toy source tables (SQL Server, run as a sysadmin / db_owner).
EXEC sys.sp_cdc_enable_db;
GO
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',    @role_name = NULL, @supports_net_changes = 0;
GO
-- cdc.lsn_time_mapping (start_lsn -> tran_end_time) already exists in every CDC-enabled database;
-- the dbt source `lsn_time_mapping` reads it directly.
