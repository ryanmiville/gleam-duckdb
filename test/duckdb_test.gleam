import duckdb
import gleam/dynamic/decode
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn simple_test() {
  let row_decoder = {
    use i <- decode.field(0, decode.int)
    use s <- decode.field(1, decode.string)
    decode.success(#(i, s))
  }

  use conn <- duckdb.with_connection("")
  duckdb.query("CREATE OR REPLACE TABLE test (i INTEGER, s STRING)")
  |> duckdb.execute(conn)
  |> should.be_ok

  duckdb.query("INSERT INTO test VALUES (42, 'hello!')")
  |> duckdb.execute(conn)
  |> should.be_ok

  duckdb.query("SELECT * FROM test")
  |> duckdb.returning(row_decoder)
  |> duckdb.execute(conn)
  |> should.equal(Ok([#(42, "hello!")]))
}
