-- A 128- or 256-bit repeat count above `UINT64_MAX` must not be truncated into a small one.
SELECT repeat('x', toUInt128('18446744073709551617')); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT repeat('x', toUInt256('18446744073709551617')); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT repeat('x', toInt128('18446744073709551617')); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT repeat(materialize('x'), toUInt128('18446744073709551617')); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT repeat('x', materialize(toUInt128('18446744073709551617'))); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT repeat(materialize('x'), materialize(toUInt256('18446744073709551617'))); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT repeat('ab', toUInt128(3)), repeat(materialize('ab'), materialize(toInt256(2)));
