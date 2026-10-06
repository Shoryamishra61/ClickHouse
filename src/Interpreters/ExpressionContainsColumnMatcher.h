#pragma once

#include <Parsers/IAST_fwd.h>

#include <functional>

namespace DB
{

class ASTIdentifier;

/// Whether the qualifier `x` of a qualified matcher `x.*` or `x.COLUMNS(...)` names a column (e.g. of type `Tuple`).
using IsColumnQualifier = std::function<bool(const ASTIdentifier & qualifier)>;

/// The first column matcher (`*`, `t.*`, `COLUMNS(...)`, `t.COLUMNS(...)`) in the expression AST, or nullptr.
///
/// A matcher expands into the columns of the query's table sources, so it has no meaning in an expression that
/// is evaluated over a single table on its own (a row policy, `additional_table_filters`). It can hide behind a
/// SQL UDF that is inlined into the expression later, which is caught by descending into the UDF body. A matcher
/// inside a nested subquery resolves against that subquery's own tables, so subqueries are skipped.
///
/// A qualified matcher whose qualifier is a column, e.g. `tup.*` for a `Tuple` column `tup`, expands into the
/// elements of that column rather than into table columns, so it is not reported.
const IAST * findColumnMatcherInExpression(const IAST & ast, const IsColumnQualifier & is_column_qualifier);

}
