#pragma once

#include <Parsers/IAST_fwd.h>

#include <cstddef>


namespace DB
{

/// Counts the AST elements that the propagation of `WITH` elements copies into a query and throws
/// `TOO_BIG_AST` once they exceed `max_expanded_ast_elements`. A copied element can itself carry copies
/// of the elements declared before it, so a `WITH` list whose elements refer to the preceding ones, or a
/// chain of common table expressions each reading the previous one twice, grows exponentially.
/// The limit applies to the whole expanded query: the first copy also charges the elements of `root`,
/// the query being expanded, so a pass over a query that is already large cannot add another full limit
/// of copies, and successive passes cannot each spend the full limit. A query that copies nothing is not
/// counted at all. Zero means no limit.
class ExpandedASTBudget
{
public:
    ExpandedASTBudget(size_t max_elements_, const IAST & root_) : max_elements(max_elements_), root(root_) {}

    ASTPtr clone(const ASTPtr & ast);

private:
    const size_t max_elements;
    const IAST & root;
    size_t used_elements = 0;
    bool root_counted = false;
};

}
