UPDATE sinex_shell_nodes n
SET {field} = (
    SELECT count(*)
    FROM sinex_shell_nodes x
    WHERE x.source = 1 - n.source
      AND x.host = n.host
      AND x.command_hash = n.command_hash
      AND x.command = n.command
      AND x.at_ns >= n.at_ns - $2::bigint
      AND x.at_ns <= n.at_ns + $2::bigint
      AND (NOT $3 OR (NOT x.paired AND NOT n.paired))
)
WHERE n.id = ANY($1)
