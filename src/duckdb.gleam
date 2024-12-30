import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode.{type Decoder}
import gleam/list
import gleam/result

pub type Database

pub type Connection

pub type Value

@external(erlang, "duckdb_ffi", "null")
pub fn null() -> Value

@external(erlang, "duckdb_ffi", "coerce")
pub fn bool(a: Bool) -> Value

@external(erlang, "duckdb_ffi", "coerce")
pub fn int(a: Int) -> Value

@external(erlang, "duckdb_ffi", "coerce")
pub fn float(a: Float) -> Value

@external(erlang, "duckdb_ffi", "coerce")
pub fn text(a: String) -> Value

@external(erlang, "duckdb_ffi", "coerce")
pub fn bytea(a: BitArray) -> Value

pub fn array(converter: fn(a) -> Value, values: List(a)) -> Value {
  list.map(values, converter)
  |> coerce_value
}

@external(erlang, "duckdb_ffi", "coerce")
fn coerce_value(a: anything) -> Value

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
  Query(sql: String, parameters: List(Value), row_decoder: Decoder(row_type))
}

pub fn query(sql: String) -> Query(Nil) {
  Query(sql:, parameters: [], row_decoder: decode.success(Nil))
}

pub fn returning(query: Query(a), decoder: Decoder(b)) -> Query(b) {
  let Query(sql:, parameters:, row_decoder: _) = query
  Query(sql:, parameters:, row_decoder: decoder)
}

/// Push a new query parameter value for the query.
pub fn parameter(query: Query(a), parameter: Value) -> Query(a) {
  Query(..query, parameters: [parameter, ..query.parameters])
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
