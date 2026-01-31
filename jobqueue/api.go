package jobqueue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GetCounts returns total + by-status counts, with optional filters (group/type/status).
func (q *Queue) GetCounts(ctx context.Context, f JobQuery) (Counts, error) {
	where, args := buildWhere(f, 1, false)

	// Total
	var out Counts
	out.ByStatus = make(map[JobStatus]int64)

	totalSQL := `SELECT count(*) FROM jobs ` + where
	if err := q.pool.QueryRow(ctx, totalSQL, args...).Scan(&out.Total); err != nil {
		return Counts{}, err
	}

	// By status
	rows, err := q.pool.Query(ctx, `SELECT status, count(*) FROM jobs `+where+` GROUP BY status`, args...)
	if err != nil {
		return Counts{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var s JobStatus
		var c int64
		if err := rows.Scan(&s, &c); err != nil {
			return Counts{}, err
		}
		out.ByStatus[s] = c
	}
	return out, rows.Err()
}

// GetTypeStatusCounts returns counts grouped by (type, status).
func (q *Queue) GetTypeStatusCounts(ctx context.Context, f JobQuery, limitTypes int) ([]TypeStatusCount, error) {
	where, args := buildWhere(f, 1, false)

	lim := ""
	if limitTypes > 0 {
		// Limit types by total volume (approx): take top N types in a CTE.
		lim = fmt.Sprintf(`
			WITH top_types AS (
			  SELECT type
			  FROM jobs %s
			  GROUP BY type
			  ORDER BY count(*) DESC
			  LIMIT %d
			)
		`, where, limitTypes)
		where = where + " AND type IN (SELECT type FROM top_types)"
	}

	sql := lim + `
		SELECT type, status, count(*)
		FROM jobs ` + where + `
		GROUP BY type, status
		ORDER BY type, status
	`

	rows, err := q.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TypeStatusCount
	for rows.Next() {
		var r TypeStatusCount
		if err := rows.Scan(&r.Type, &r.Status, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListGroups returns top groups by queued backlog, plus next occurred_at.
// TODO: Remove occurred_at
func (q *Queue) ListGroups(ctx context.Context, jobType string, limit, offset int) ([]GroupCounts, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	args := []any{}
	where := "WHERE 1=1"
	if jobType != "" {
		args = append(args, jobType)
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM jobs j2 WHERE j2.job_group=g.job_group AND j2.type=$%d)", len(args))
	}

	args = append(args, limit, offset)

	sql := `
		SELECT
		  g.job_group,
		  COALESCE(SUM(CASE WHEN j.status='queued' THEN 1 ELSE 0 END),0) AS queued,
		  COALESCE(SUM(CASE WHEN j.status='running' THEN 1 ELSE 0 END),0) AS running,
		  COALESCE(SUM(CASE WHEN j.status='dead' THEN 1 ELSE 0 END),0) AS dead,
		FROM job_groups g
		LEFT JOIN jobs j ON j.job_group=g.job_group
		` + where + `
		GROUP BY g.job_group
		ORDER BY queued DESC, next_occurred_at NULLS LAST, g.job_group
		LIMIT $` + fmt.Sprint(len(args)-1) + ` OFFSET $` + fmt.Sprint(len(args)) + `
	`

	rows, err := q.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GroupCounts
	for rows.Next() {
		var r GroupCounts
		if err := rows.Scan(&r.JobGroup, &r.Queued, &r.Running, &r.Dead); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RecentErrors returns “top errors” for failed/dead jobs within a time window.
// Groups by (type, status, error_key) and optionally job_group.
func (q *Queue) RecentErrors(ctx context.Context, window time.Duration, includeGroup bool, limit int) ([]ErrorSummary, error) {
	if window <= 0 {
		window = 24 * time.Hour
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	// ErrorKey: stable-ish group key to avoid grouping by huge strings.
	// We take md5(last_error) and show a truncated error text for display.
	groupCols := "type, status"
	selectCols := "type, NULL::text as job_group, status"
	if includeGroup {
		groupCols = "type, job_group, status"
		selectCols = "type, job_group, status"
	}

	sql := `
		SELECT
		  ` + selectCols + `,
		  substr(md5(COALESCE(last_error,'')), 1, 12) AS error_key,
		  left(COALESCE(last_error,''), 240) AS error_text,
		  count(*) AS cnt,
		  max(finished_at) AS last_seen
		FROM jobs
		WHERE status IN ('failed','dead')
		  AND finished_at IS NOT NULL
		  AND finished_at >= now() - ($1::bigint * interval '1 microsecond')
		  AND COALESCE(last_error,'') <> ''
		GROUP BY ` + groupCols + `, error_key, error_text
		ORDER BY cnt DESC, last_seen DESC
		LIMIT $2
	`

	rows, err := q.pool.Query(ctx, sql, micros(window), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ErrorSummary
	for rows.Next() {
		var r ErrorSummary
		var maybeGroup *string
		if includeGroup {
			if err := rows.Scan(&r.Type, &maybeGroup, &r.Status, &r.ErrorKey, &r.ErrorText, &r.Count, &r.LastSeen); err != nil {
				return nil, err
			}
			r.JobGroup = maybeGroup
		} else {
			if err := rows.Scan(&r.Type, &maybeGroup, &r.Status, &r.ErrorKey, &r.ErrorText, &r.Count, &r.LastSeen); err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListJobs returns a page of jobs + total count (for UI pagination).
func (q *Queue) ListJobs(ctx context.Context, f JobQuery) (JobListPage, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 500 {
		f.Limit = 500
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	if f.Sort == "" {
		f.Sort = SortOrderDesc
	}

	where, args := buildWhere(f, 1, true)

	// Total count query
	var total int64
	if err := q.pool.QueryRow(ctx, `SELECT count(*) FROM jobs `+where, args...).Scan(&total); err != nil {
		return JobListPage{}, err
	}

	orderBy, err := sortClause(f.Sort)
	if err != nil {
		return JobListPage{}, err
	}

	// Select columns
	cols := `
		id, job_group, type, occurred_at, run_after, status, payload,
		attempts, max_attempts, locked_by, locked_until, started_at, finished_at, last_error
	`
	if !f.IncludePayload {
		cols = strings.Replace(cols, "payload,", "NULL::jsonb AS payload,", 1)
	}

	args = append(args, f.Limit, f.Offset)
	limitArg := len(args) - 1
	offsetArg := len(args)

	sql := `SELECT ` + cols + ` FROM jobs ` + where + ` ` + orderBy +
		fmt.Sprintf(" LIMIT $%d OFFSET $%d", limitArg, offsetArg)

	rows, err := q.pool.Query(ctx, sql, args...)
	if err != nil {
		return JobListPage{}, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return JobListPage{}, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return JobListPage{}, err
	}

	return JobListPage{Jobs: jobs, Total: total}, nil
}

// GetJob fetches a single job by id.
func (q *Queue) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	row := q.pool.QueryRow(ctx, `
		SELECT
		  id, job_group, type, occurred_at, run_after, status, payload,
		  attempts, max_attempts, locked_by, locked_until, started_at, finished_at, last_error
		FROM jobs
		WHERE id=$1
	`, id)
	return scanJob(row)
}

// --- helpers ---

func buildWhere(f JobQuery, startArg int, allowTime bool) (string, []any) {
	where := "WHERE 1=1"
	args := []any{}

	if f.JobGroup != "" {
		args = append(args, f.JobGroup)
		where += fmt.Sprintf(" AND job_group = $%d", startArg+len(args)-1)
	}
	if f.Type != "" {
		args = append(args, f.Type)
		where += fmt.Sprintf(" AND type = $%d", startArg+len(args)-1)
	}
	if len(f.Statuses) > 0 {
		// Build IN ($x, $y, ...)
		place := make([]string, 0, len(f.Statuses))
		for _, s := range f.Statuses {
			args = append(args, s)
			place = append(place, fmt.Sprintf("$%d", startArg+len(args)-1))
		}
		where += " AND status IN (" + strings.Join(place, ",") + ")"
	}

	if allowTime && (f.Since != nil || f.Until != nil) {
		// Time filter uses finished_at if sorting by finished, started_at if started, else ordering_seq.
		col := "ordering_seq"
		switch f.Sort {
		case SortFinishedDesc:
			col = "finished_at"
		case SortStartedDesc:
			col = "started_at"
			// case SortCreatedDesc:
			// 	col = "created_at" // requires column
		}

		if f.Since != nil {
			args = append(args, *f.Since)
			where += fmt.Sprintf(" AND %s >= $%d", col, startArg+len(args)-1)
		}
		if f.Until != nil {
			args = append(args, *f.Until)
			where += fmt.Sprintf(" AND %s <= $%d", col, startArg+len(args)-1)
		}
	}

	if f.IDPrefix != "" {
		args = append(args, strings.TrimSpace(f.IDPrefix)+"%")
		where += fmt.Sprintf(" AND id::text ILIKE $%d", startArg+len(args)-1)
	}

	if f.ErrorContains != "" {
		args = append(args, "%"+strings.TrimSpace(f.ErrorContains)+"%")
		where += fmt.Sprintf(" AND COALESCE(last_error,'') ILIKE $%d", startArg+len(args)-1)
	}

	if f.LockedBy != "" {
		args = append(args, strings.TrimSpace(f.LockedBy))
		where += fmt.Sprintf(" AND locked_by = $%d", startArg+len(args)-1)
	}

	if f.ExpiredRunning {
		where += " AND status='running' AND locked_until IS NOT NULL AND locked_until < now()"
	}

	return where, args
}

func sortClause(s JobSort) (string, error) {
	switch s {
	case SortOrderAsc:
		return "ORDER BY ordering_seq ASC", nil
	case SortOrderDesc:
		return "ORDER BY ordering_seq DESC", nil
	case SortStartedDesc:
		return "ORDER BY started_at DESC NULLS LAST", nil
	case SortFinishedDesc:
		return "ORDER BY finished_at DESC NULLS LAST", nil
	default:
		return "", fmt.Errorf("pgjobq: unknown sort %q", s)
	}
}
