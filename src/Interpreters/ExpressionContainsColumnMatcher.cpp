#include <Interpreters/ExpressionContainsColumnMatcher.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>

namespace DB
{

namespace
{

const IAST * getMatcherQualifier(const IAST & ast)
{
    if (const auto * matcher = ast.as<ASTQualifiedAsterisk>())
        return matcher->qualifier.get();
    if (const auto * matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
        return matcher->qualifier.get();
    if (const auto * matcher = ast.as<ASTQualifiedColumnsListMatcher>())
        return matcher->qualifier.get();
    return nullptr;
}

bool isColumnMatcher(const IAST & ast, const IsColumnQualifier & is_column_qualifier)
{
    if (ast.as<ASTAsterisk>() || ast.as<ASTColumnsRegexpMatcher>() || ast.as<ASTColumnsListMatcher>())
        return true;

    const auto * qualifier = getMatcherQualifier(ast);
    if (!qualifier)
        return false;

    const auto * qualifier_identifier = qualifier->as<ASTIdentifier>();
    return !qualifier_identifier || !is_column_qualifier(*qualifier_identifier);
}

const IAST * findColumnMatcherInExpressionImpl(
    const IAST & ast, const IsColumnQualifier & is_column_qualifier, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (isColumnMatcher(ast, is_column_qualifier))
        return &ast;

    if (const auto * function = ast.as<ASTFunction>())
    {
        /// Each body is walked at most once, so that a cycle among them cannot make this recurse forever.
        auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
        if (udf_body && visited_udfs.insert(function->name).second)
        {
            if (const auto * matcher = findColumnMatcherInExpressionImpl(*udf_body, is_column_qualifier, visited_udfs))
                return matcher;
        }
    }

    for (const auto & child : ast.children)
    {
        if (child->as<ASTSelectQuery>())
            continue;
        if (const auto * matcher = findColumnMatcherInExpressionImpl(*child, is_column_qualifier, visited_udfs))
            return matcher;
    }

    return nullptr;
}

}

const IAST * findColumnMatcherInExpression(const IAST & ast, const IsColumnQualifier & is_column_qualifier)
{
    UnorderedSetWithMemoryTracking<String> visited_udfs;
    return findColumnMatcherInExpressionImpl(ast, is_column_qualifier, visited_udfs);
}

}
