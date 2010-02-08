-module(erldis_binaries).

-export([to_binary/1, join/2]).

to_binary(X) when is_list(X) -> list_to_binary(X);
to_binary(X) when is_atom(X) -> list_to_binary(atom_to_list(X));
to_binary(X) when is_binary(X) -> X;
to_binary(X) when is_integer(X) -> list_to_binary(integer_to_list(X));
to_binary(X) when is_float(X) -> list_to_binary(float_to_list(X));
to_binary(X) -> term_to_binary(X).

join([], _)->
	<<>>;
join(Array, Sep)->
	F = fun(Elem, Acc) ->
			E2 = to_binary(Elem),
			<<Acc/binary, Sep/binary, E2/binary>>
		end,
	
	Sz = size(Sep),
	<<_:Sz/bytes, Result/binary>> = lists:foldl(F, <<>>, Array),
	Result.