import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode.{type Decoder}
import gleam/list
import gleam/result

pub type Database

pub type Connection

type PreparedStatement

type BindResult

pub opaque type Value {
  Value(bind: fn(PreparedStatement, Int) -> BindResult)
}

pub fn null() -> Value {
  Value(fn(stmt, idx) { bind_null(stmt, idx) })
}

pub fn bool(a: Bool) -> Value {
  Value(fn(stmt, idx) { bind_bool(stmt, idx, a) })
}

pub fn int(a: Int) -> Value {
  Value(fn(stmt, idx) { bind_int(stmt, idx, a) })
}

pub fn float(a: Float) -> Value {
  Value(fn(stmt, idx) { bind_float(stmt, idx, a) })
}

pub fn varchar(a: String) -> Value {
  Value(fn(stmt, idx) { bind_varchar(stmt, idx, a) })
}

pub fn array(converter: fn(a) -> Value, values: List(a)) -> Value {
  list.map(values, converter)
  |> coerce_value
}

@external(erlang, "educkdb", "bind_int64")
fn bind_int(statement: PreparedStatement, index: Int, a: Int) -> BindResult

@external(erlang, "educkdb", "bind_float")
fn bind_float(statement: PreparedStatement, index: Int, a: Float) -> BindResult

@external(erlang, "educkdb", "bind_boolean")
fn bind_bool(statement: PreparedStatement, index: Int, a: Bool) -> BindResult

@external(erlang, "educkdb", "bind_null")
fn bind_null(statement: PreparedStatement, index: Int) -> BindResult

@external(erlang, "educkdb", "bind_varchar")
fn bind_varchar(
  statement: PreparedStatement,
  index: Int,
  a: String,
) -> BindResult

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

@external(erlang, "educkdb", "prepare")
fn prepare(
  connection: Connection,
  sql: String,
) -> Result(PreparedStatement, DuckDbError)

pub fn execute(
  query query: Query(a),
  on connection: Connection,
) -> Result(List(a), DuckDbError) {
  let rows = case query.parameters {
    [] -> run_query(query.sql, connection)
    _ -> prepared_statement_rows(query, connection)
  }
  use rows <- result.try(rows)
  use rows <- result.try(
    list.try_map(over: rows, with: decode.run(_, query.row_decoder))
    |> result.map_error(UnexpectedResultType),
  )
  Ok(rows)
}

@external(erlang, "duckdb_ffi", "execute_prepared_statement")
fn execute_prepared(
  statement: PreparedStatement,
) -> Result(List(Dynamic), DuckDbError)

fn prepared_statement_rows(
  query query: Query(a),
  on connection: Connection,
) -> Result(List(Dynamic), DuckDbError) {
  use statement <- result.try(prepare(connection, query.sql))
  let statement = bind_parameters(statement, query.parameters)
  use rows <- result.try(execute_prepared(statement))
  Ok(rows)
}

fn bind_parameters(
  statement: PreparedStatement,
  parameters: List(Value),
) -> PreparedStatement {
  parameters
  |> list.reverse
  |> list.index_fold(statement, fn(acc, p, idx) {
    let _ = p.bind(acc, idx + 1)
    acc
  })
}

@external(erlang, "duckdb_ffi", "open")
pub fn open(path: String) -> Result(Database, DuckDbError)

@external(erlang, "duckdb_ffi", "close")
pub fn close(database: Database) -> Result(Nil, DuckDbError)

@external(erlang, "duckdb_ffi", "connect")
pub fn connect(database: Database) -> Result(Connection, DuckDbError)

@external(erlang, "duckdb_ffi", "disconnect")
pub fn disconnect(connection: Connection) -> Result(Nil, DuckDbError)
