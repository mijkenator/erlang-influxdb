%% @private
-module(influxdb_app).

-behaviour(application).
-export([
    start/2,
    stop/1,
    start/0,
    test/0
]).

test() ->
    Config = influxdb_config:new(#{host => "10.0.0.85", username => "root", password => "root"}),
    influxdb:write(Config#{database => "mkh_data"},
        [{"mkh_test",#{"region" => "af-west", "host" => "server01"},#{"value" => 0.64}}]).

start() ->
    application:start(inets),
    application:start(jsone),
    buoy_app:start(),
    application:start(influxdb).

-spec start(start_type(), term()) -> {ok, pid()} | {ok, pid(), State :: term()} | {error, term()}.
-type start_type() :: normal | {takeover, node()} | {failover, node()}.
start(_StartType, _StartArgs) ->
    {ok, _} = inets:start(httpc, [
        {profile, influxdb_query},
        {max_sessions, 10},
        {max_keep_alive_length, 0},
        {max_pipeline_length, 0}
    ]),
    {ok, _} = inets:start(httpc, [
        {profile, influxdb_write},
        {max_sessions, 10},
        {max_keep_alive_length, 10},
        {max_pipeline_length, 0}
    ]),
    buoy_pool:start(buoy_utils:parse_url(<<"http://10.0.0.85:8086">>), [{pool_size, 30}]),
    influxdb_sup:start_link().

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    inets:stop(httpc, influxdb_query),
    inets:stop(httpc, influxdb_write),
    ok.
