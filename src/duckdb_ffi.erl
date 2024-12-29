-module(duckdb_ffi).

-export([open/1, close/1, connect/1, disconnect/1, query/2]).

open(Filename) ->
        educkdb:open(
                unicode:characters_to_list(Filename)).

close(Database) ->
        case educkdb:close(Database) of
                ok ->
                        {ok, nil};
                {error, Msg} ->
                        {error, Msg}
        end.

connect(Database) ->
        educkdb:connect(Database).

disconnect(Connection) ->
        case educkdb:disconnect(Connection) of
                ok ->
                        {ok, nil};
                {error, Msg} ->
                        {error, Msg}
        end.

query(Sql, Connection) ->
        case educkdb:query(Connection, Sql) of
                {ok, Res} ->
                        result_extract(educkdb:result_extract(Res));
                {error, Msg} ->
                        {error, Msg}
        end.

result_extract(Res) ->
        case Res of
                {ok, _, Rows} ->
                        {ok, Rows};
                {error, Msg} ->
                        {error, Msg}
        end.
