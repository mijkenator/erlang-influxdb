-module(influxdb_http).
-export([
    post/7,
    post_buoy/7
]).
-export_type([
    result/0,
    series/0
]).


-spec post(client(), binary(), string(), string(), string(), iodata(), timeout()) ->
      ok
    | {ok, [result()]}
    | {error, {not_found, string()}}
| {error, {server_error, string()}}.
-type client() :: query | write.
-type result() :: [series()].
-type series() :: #{name := binary(), columns := [binary()], rows := [tuple()], tags => #{binary() => binary()}}.
post(Client, Url, Username, Password, ContentType, Body, Timeout) ->
    Authorization = "Basic " ++ base64:encode_to_string(Username ++ ":" ++ Password),
    Headers = [{"Authorization", Authorization}],
    %io:format("POST: ~p ~n", [{Client, Url, Authorization, Headers, ContentType, Body, Timeout}]),
    case httpc:request(post,
            {binary_to_list(Url), Headers, ContentType, iolist_to_binary(Body)},
            [{timeout, Timeout}],
            [{body_format, binary}],
            profile(Client)) of
        {ok, {{_, RespCode, _}, RespHeaders, RespBody}} ->
            response(RespCode, RespHeaders, RespBody);
        {error, Reason} ->
            erlang:exit(Reason)
    end.

post_buoy(_, Url, Username, Password, ContentType, Body, Timeout) ->
    BLP =  list_to_binary(base64:encode_to_string(Username ++ ":" ++ Password)),
    Headers = [{<<"Authorization">>, <<"Basic ", BLP/binary>>},
               {<<"Content-Type">>, list_to_binary(ContentType)}],
    BUrl = buoy_utils:parse_url(Url),
    case buoy:post(BUrl, Headers, Body, Timeout) of
        {ok, OK} -> buoy_resp(OK);
        Error    -> erlang:exit(Error)
    end.

%% Internals

buoy_resp({buoy_resp, done, RespBody, _, RespHeaders, _RB, RespCode}) ->
    response(RespCode, RespHeaders, RespBody);
buoy_resp(NOK) -> {error, NOK}.

profile(query) ->
    influxdb_query;
profile(write) ->
    influxdb_write.


response(200, _, Body) ->
    case results(jsone:decode(Body)) of
        [] -> ok;
        Results -> {ok, Results}
    end;
response(204, _, _) ->
    ok;
response(400, _, Body) ->
    #{<<"error">> := Message} = jsone:decode(Body),
    erlang:error({bad_request, unicode:characters_to_list(Message)});
response(404, _, Body) ->
    #{<<"error">> := Message} = jsone:decode(Body),
    {error, {not_found, unicode:characters_to_list(Message)}};
response(500, _, Body) ->
    #{<<"error">> := Message} = jsone:decode(Body),
    {error, {server_error, unicode:characters_to_list(Message)}}.


results(#{<<"results">> := Results}) ->
    [series(Series) || #{<<"series">> := Series} <- Results].


series(Series) ->
    [maps:fold(fun
        (<<"name">>, Name, Acc) -> Acc#{name => Name};
        (<<"tags">>, Tags, Acc) -> Acc#{tags => Tags};
        (<<"columns">>, Columns, Acc) -> Acc#{columns => Columns};
        (<<"values">>, Values, Acc) -> Acc#{rows => [list_to_tuple(Value) || Value <- Values]}
    end, #{}, S) || S <- Series].
