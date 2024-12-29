import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode.{type Decoder}
import gleam/io
import gleam/list
import gleam/result

pub fn main() {
  let row_decoder = {
    use i <- decode.field(0, decode.int)
    use s <- decode.field(1, decode.string)
    decode.success(#(i, s))
  }
  use conn <- with_connection("")
  let assert Ok(_) =
    query("CREATE OR REPLACE TABLE test (i INTEGER, s STRING)")
    |> execute(conn)
  let assert Ok(_) =
    query("INSERT INTO test VALUES (42, 'hello!')")
    |> execute(conn)
  let assert Ok([#(42, "hello!")]) =
    query("SELECT * FROM test")
    |> returning(row_decoder)
    |> execute(conn)
    |> io.debug
}

pub type Database

pub type Connection

pub type Value

pub type DuckDbError {
  DuckDbError(message: String)
  UnexpectedResultType(List(decode.DecodeError))
}

pub fn with_connection(path: String, f: fn(Connection) -> a) -> a {
  let assert Ok(database) = open(path)
  let assert Ok(connection) = connect(database)
  let value = f(connection)
  let assert Ok(_) = disconnect(connection)
  let assert Ok(_) = close(database)
  value
}

@external(erlang, "duckdb_ffi", "query")
pub fn run_query(
  sql: String,
  connection: Connection,
) -> Result(List(Dynamic), DuckDbError)

pub opaque type Query(row_type) {
  Query(sql: String, row_decoder: Decoder(row_type))
}

pub fn query(sql: String) -> Query(Nil) {
  Query(sql:, row_decoder: decode.success(Nil))
}

pub fn returning(query: Query(a), decoder: Decoder(b)) -> Query(b) {
  let Query(sql:, row_decoder: _) = query
  Query(sql:, row_decoder: decoder)
}

pub fn execute(
  query query: Query(a),
  on connection: Connection,
) -> Result(List(a), DuckDbError) {
  use rows <- result.try(run_query(query.sql, connection))
  use rows <- result.try(
    list.try_map(over: rows, with: decode.run(_, query.row_decoder))
    |> result.map_error(UnexpectedResultType),
  )
  Ok(rows)
}

@external(erlang, "duckdb_ffi", "open")
pub fn open(path: String) -> Result(Database, DuckDbError)

@external(erlang, "duckdb_ffi", "close")
pub fn close(database: Database) -> Result(Nil, DuckDbError)

@external(erlang, "duckdb_ffi", "connect")
pub fn connect(database: Database) -> Result(Connection, DuckDbError)

@external(erlang, "duckdb_ffi", "disconnect")
pub fn disconnect(connection: Connection) -> Result(Nil, DuckDbError)
