SELECT
    a.pid AS waiting_pid,
    a.usename AS waiting_user,
    a.query AS waiting_query,
    a.state AS waiting_state,
    l.locktype,
    l.mode AS lock_mode,
    l.granted AS lock_granted,
    t.relname AS table_name,
    b.pid AS blocking_pid,
    b.query AS blocking_query,
    b.usename AS blocking_user
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
LEFT JOIN pg_locks bl ON l.locktype = bl.locktype AND l.database IS NOT DISTINCT FROM bl.database
LEFT JOIN pg_stat_activity b ON bl.pid = b.pid AND bl.granted = true
LEFT JOIN pg_class t ON t.oid = l.relation
WHERE a.state = 'active'
  AND (l.granted = false OR l.granted IS NULL)
ORDER BY a.pid;
