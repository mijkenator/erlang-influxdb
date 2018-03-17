-module(influxdb).
-export([
    query/2,
    query/3,
    query/4,
    write/2,
    write/3,
    write_buoy/3
]).
-export_type([
    config/0,
    time_unit/0,
    query/0,
    query_parameters/0,
    point/0
]).


-type config() :: influxdb_config:config().
-type time_unit() :: hour | minute | second | millisecond | microsecond | nanosecond.


-spec query(config(), query()) ->
      ok
    | {ok, [result()]}
    | {error, {not_found, string()}}
    | {error, {server_error, string()}}.
query(Config, Query) ->
    query(Config, Query, #{}, #{}).


-spec query(config(), query(), query_parameters()) ->
      ok
    | {ok, [result()]}
    | {error, {not_found, string()}}
    | {error, {server_error, string()}}.
query(Config, Query, Parameters) ->
    query(Config, Query, Parameters, #{}).


-spec query(config(), query(), query_parameters(), query_options()) ->
      ok
    | {ok, [result()]}
    | {error, {not_found, string()}}
    | {error, {server_error, string()}}.
-type query() :: iodata().
-type query_parameters() :: #{atom() => atom() | binary() | number()}.
-type query_options() :: #{
    timeout => timeout(),
    precision => time_unit(),
    retention_policy => iodata()
}.
-type result() :: influxdb_http:result().
query(#{host := Host, port := Port, username := Username, password := Password} = Config, Query, Parameters, Options) when is_map(Parameters), is_map(Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    Url = influxdb_uri:encode(#{
        scheme => "http",
        host => Host,
        port => Port,
        path => "/query",
        query => url_query(Config, Options)
    }),
    Body = influxdb_uri:encode_query(#{
        q => Query,
        params => jsone:encode(Parameters)
    }),
    influxdb_http:post(query, Url, Username, Password, "application/x-www-form-urlencoded", Body, Timeout).


url_query(Config, Options) ->
    maps:fold(fun
        (precision, Value, Acc) -> maps:put("epoch", precision(Value), Acc);
        (retention_policy, Value, Acc) -> maps:put("rp", Value, Acc);
        (_Key, _Value, Acc) -> Acc
    end, default_url_query(Config), Options).


default_url_query(#{database := Database}) ->
    #{"db" => Database, "epoch" => precision(nanosecond)};
default_url_query(#{}) ->
    #{"epoch" => precision(nanosecond)}.


precision(hour) -> "h";
precision(minute) -> "m";
precision(second) -> "s";
precision(millisecond) -> "ms";
precision(microsecond) -> "u";
precision(nanosecond) -> "ns".


-spec write(config(), [point()]) ->
      ok
    | {error, {not_found, string()}}
    | {error, {server_error, string()}}.
write(Config, Measurements) ->
    write(Config, Measurements, #{}).


-spec write(config(), [point()], write_options()) ->
      ok
    | {error, {not_found, string()}}
    | {error, {server_error, string()}}.
-type point() :: influxdb_line_encoding:point().
-type write_options() :: #{
    timeout => timeout(),
    precision => time_unit(),
    retention_policy => string()
}.
write(#{host := Host, port := Port, username := Username, password := Password, database := Database}, Measurements, Options) ->
    Timeout = maps:get(timeout, Options, infinity),
    Url = influxdb_uri:encode(#{
        scheme => "http",
        host => Host,
        port => Port,
        path => "/write",
        query => maps:fold(fun
            (precision, Value, Acc) -> maps:put("precision", precision(Value), Acc);
            (retention_policy, Value, Acc) -> maps:put("rp", Value, Acc);
            (_Key, _Value, Acc) -> Acc
        end, #{"db" => Database}, Options)
    }),
    Body = influxdb_line_encoding:encode(Measurements),
    influxdb_http:post(write, Url, Username, Password, "application/octet-stream", Body, Timeout).

write_buoy(#{host := Host, port := Port, username := Username, password := Password, database := Database}, Measurements, Options) ->
    Timeout = maps:get(timeout, Options, 1000),
    Url = influxdb_uri:encode(#{
        scheme => "http",
        host => Host,
        port => Port,
        path => "/write",
        query => maps:fold(fun
            (precision, Value, Acc) -> maps:put("precision", precision(Value), Acc);
            (retention_policy, Value, Acc) -> maps:put("rp", Value, Acc);
            (_Key, _Value, Acc) -> Acc
        end, #{"db" => Database}, Options)
    }),
    Body = influxdb_line_encoding:encode(Measurements),
    influxdb_http:post_buoy(write, Url, Username, Password, "application/octet-stream", Body, Timeout).

