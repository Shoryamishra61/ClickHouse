-- With `short_circuit_function_evaluation = 'disable'` the analyzer must not suppress exceptions
-- in the unreachable constant arguments of `coalesce` and `ifNull`: all the arguments are evaluated eagerly.

SET enable_analyzer = 1;

SET short_circuit_function_evaluation = 'disable';

SELECT coalesce(1, intDiv(1, 0)); -- { serverError ILLEGAL_DIVISION }
SELECT coalesce(toNullable(1), intDiv(1, 0)); -- { serverError ILLEGAL_DIVISION }
SELECT ifNull(toNullable(1), intDiv(1, 0)); -- { serverError ILLEGAL_DIVISION }
SELECT coalesce(number, intDiv(1, 0)) FROM numbers(2); -- { serverError ILLEGAL_DIVISION }

-- Without the exceptions the result is the same.
SELECT coalesce(NULL, 2, 3), ifNull(toNullable(4), 5);

SET short_circuit_function_evaluation = 'enable';

SELECT coalesce(1, intDiv(1, 0)), ifNull(toNullable(1), intDiv(1, 0));
