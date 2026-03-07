use arrow_flight::error::Result;
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::SqlInfo;

pub fn build_sql_info_data() -> Result<arrow_flight::sql::metadata::SqlInfoData> {
    let mut builder = SqlInfoDataBuilder::new();

    builder.append(SqlInfo::FlightSqlServerName, "demoflight");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "54");
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfo::FlightSqlServerSql, false);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(SqlInfo::FlightSqlServerTransaction, 0i32);
    builder.append(SqlInfo::FlightSqlServerCancel, false);
    builder.append(SqlInfo::FlightSqlServerStatementTimeout, 0i32);
    builder.append(SqlInfo::FlightSqlServerTransactionTimeout, 0i32);

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_sql_info() {
        let info = build_sql_info_data().unwrap();
        let batch = info.record_batch(vec![
            SqlInfo::FlightSqlServerName as u32,
            SqlInfo::FlightSqlServerVersion as u32,
        ]);

        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
