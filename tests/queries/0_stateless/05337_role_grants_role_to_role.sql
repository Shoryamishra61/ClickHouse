-- Test: system.role_grants when a role is granted to another role.
-- StorageSystemRoleGrants.fillData (src/Storages/System/StorageSystemRoleGrants.cpp)
-- has two grantee type branches: USER (lines 72-77, always taken in CI) and ROLE
-- (lines 78-84, never taken in CI). All existing tests only query role_grants for
-- users (WHERE user_name = ...). Granting a role to a role (GRANT r1 TO r2) produces
-- a row where the grantee_type == AccessEntityType::ROLE, exercising the ROLE branch
-- that populates role_name and leaves user_name as NULL.
-- Also exercises line 107: when iterating a user's granted roles, granted_role_is_default
-- checks whether the role appears in the user's DEFAULT ROLE list (default_roles->match()).
-- Users with DEFAULT ROLE NONE have a non-empty default_roles set that does not match
-- any granted role, driving the match() call that was previously short-circuited.

-- Unique role/user names to avoid conflicts with parallel test runs.
DROP ROLE IF EXISTS r_05337_parent;
DROP ROLE IF EXISTS r_05337_child;
DROP USER IF EXISTS u_05337;

CREATE ROLE r_05337_child;
CREATE ROLE r_05337_parent;

-- Grant role to role: exercises the ROLE grantee branch (lines 78-84).
GRANT r_05337_child TO r_05337_parent;

SELECT
    role_name IS NOT NULL        AS is_role_grantee,
    user_name IS NULL            AS user_name_is_null,
    granted_role_name,
    granted_role_is_default
FROM system.role_grants
WHERE role_name = 'r_05337_parent'
ORDER BY granted_role_name;

-- Grant role to user with DEFAULT ROLE NONE: exercises default_roles->match()
-- at line 107 (the user has a non-null RolesOrUsersSet that does not match the
-- granted role, so granted_role_is_default = 0).
CREATE USER u_05337 DEFAULT ROLE NONE;
GRANT r_05337_parent TO u_05337;

SELECT
    user_name IS NOT NULL        AS is_user_grantee,
    granted_role_name,
    granted_role_is_default
FROM system.role_grants
WHERE user_name = 'u_05337'
ORDER BY granted_role_name;

DROP USER u_05337;
DROP ROLE r_05337_parent;
DROP ROLE r_05337_child;
