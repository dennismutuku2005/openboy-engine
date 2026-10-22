package cronexpr

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Expression represents a parsed 5-field cron expression (minute, hour, dom, month, dow).
type Expression struct {
	raw     string
	minutes [60]bool
	hours   [24]bool
	doms    [32]bool // 1-31
	months  [13]bool // 1-12
	dows    [7]bool  // 0-6 (0=Sun)
}

// Parse parses a 5-part cron expression string.
// Supported syntax: '*', '*/n', 'a,b,c', 'a-b'.
func Parse(expr string) (*Expression, error) {
	expr = strings.TrimSpace(expr)
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d in '%s'", len(fields), expr)
	}

	e := &Expression{raw: expr}

	if err := parseField(fields[0], 0, 59, e.minutes[:]); err != nil {
		return nil, fmt.Errorf("minute field invalid: %w", err)
	}
	if err := parseField(fields[1], 0, 23, e.hours[:]); err != nil {
		return nil, fmt.Errorf("hour field invalid: %w", err)
	}
	if err := parseField(fields[2], 1, 31, e.doms[:]); err != nil {
		return nil, fmt.Errorf("day of month field invalid: %w", err)
	}
	if err := parseField(fields[3], 1, 12, e.months[:]); err != nil {
		return nil, fmt.Errorf("month field invalid: %w", err)
	}
	if err := parseDowField(fields[4], e.dows[:]); err != nil {
		return nil, fmt.Errorf("day of week field invalid: %w", err)
	}

	return e, nil
}

func parseField(field string, min, max int, target []bool) error {
	for i := range target {
		target[i] = false
	}

	parts := strings.Split(field, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Handle step: */step or range/step
		if strings.Contains(part, "/") {
			stepParts := strings.Split(part, "/")
			if len(stepParts) != 2 {
				return fmt.Errorf("invalid step syntax '%s'", part)
			}
			step, err := strconv.Atoi(stepParts[1])
			if err != nil || step <= 0 {
				return fmt.Errorf("invalid step '%s'", stepParts[1])
			}

			start := min
			end := max
			if stepParts[0] != "*" && stepParts[0] != "" {
				if strings.Contains(stepParts[0], "-") {
					rParts := strings.Split(stepParts[0], "-")
					rStart, err1 := strconv.Atoi(rParts[0])
					rEnd, err2 := strconv.Atoi(rParts[1])
					if err1 != nil || err2 != nil || rStart < min || rEnd > max || rStart > rEnd {
						return fmt.Errorf("invalid range '%s'", stepParts[0])
					}
					start = rStart
					end = rEnd
				} else {
					s, err := strconv.Atoi(stepParts[0])
					if err != nil || s < min || s > max {
						return fmt.Errorf("invalid start '%s'", stepParts[0])
					}
					start = s
				}
			}

			for i := start; i <= end; i += step {
				target[i] = true
			}
			continue
		}

		// Handle wildcards
		if part == "*" {
			for i := min; i <= max; i++ {
				target[i] = true
			}
			continue
		}

		// Handle range: a-b
		if strings.Contains(part, "-") {
			rParts := strings.Split(part, "-")
			if len(rParts) != 2 {
				return fmt.Errorf("invalid range '%s'", part)
			}
			start, err1 := strconv.Atoi(rParts[0])
			end, err2 := strconv.Atoi(rParts[1])
			if err1 != nil || err2 != nil || start < min || end > max || start > end {
				return fmt.Errorf("invalid range '%s'", part)
			}
			for i := start; i <= end; i++ {
				target[i] = true
			}
			continue
		}

		// Handle single number
		val, err := strconv.Atoi(part)
		if err != nil || val < min || val > max {
			return fmt.Errorf("value '%s' out of bounds [%d, %d]", part, min, max)
		}
		target[val] = true
	}
	return nil
}

func parseDowField(field string, target []bool) error {
	for i := range target {
		target[i] = false
	}
	parts := strings.Split(field, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "*" {
			for i := 0; i <= 6; i++ {
				target[i] = true
			}
			continue
		}
		if strings.Contains(part, "/") {
			sub := strings.Split(part, "/")
			step, err := strconv.Atoi(sub[1])
			if err != nil || step <= 0 {
				return fmt.Errorf("invalid step '%s'", sub[1])
			}
			for i := 0; i <= 6; i += step {
				target[i] = true
			}
			continue
		}
		if strings.Contains(part, "-") {
			r := strings.Split(part, "-")
			s, err1 := strconv.Atoi(r[0])
			e, err2 := strconv.Atoi(r[1])
			if err1 != nil || err2 != nil {
				return fmt.Errorf("invalid dow range: %s", part)
			}
			if s == 7 {
				s = 0
			}
			if e == 7 {
				e = 0
			}
			for i := s; i <= e; i++ {
				target[i%7] = true
			}
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil {
			return fmt.Errorf("invalid dow: %s", part)
		}
		if v == 7 {
			v = 0
		}
		if v < 0 || v > 6 {
			return fmt.Errorf("dow %d out of bounds [0, 6]", v)
		}
		target[v] = true
	}
	return nil
}

// Matches checks if the given timestamp matches the cron expression down to the minute.
func (e *Expression) Matches(t time.Time) bool {
	minute := t.Minute()
	hour := t.Hour()
	dom := t.Day()
	month := int(t.Month())
	dow := int(t.Weekday()) // 0 = Sunday

	return e.minutes[minute] &&
		e.hours[hour] &&
		e.doms[dom] &&
		e.months[month] &&
		e.dows[dow]
}

// Next finds the next time after `from` that matches the cron expression.
func (e *Expression) Next(from time.Time) time.Time {
	// Truncate to current minute and step forward minute by minute
	next := from.Truncate(time.Minute).Add(time.Minute)
	// Look up to 5 years into the future to avoid infinite loops
	limit := next.AddDate(5, 0, 0)

	for next.Before(limit) {
		if !e.months[int(next.Month())] {
			// Skip to next month
			next = time.Date(next.Year(), next.Month()+1, 1, 0, 0, 0, 0, next.Location())
			continue
		}
		if !e.doms[next.Day()] || !e.dows[int(next.Weekday())] {
			// Skip to next day
			next = time.Date(next.Year(), next.Month(), next.Day()+1, 0, 0, 0, 0, next.Location())
			continue
		}
		if !e.hours[next.Hour()] {
			// Skip to next hour
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour()+1, 0, 0, 0, next.Location())
			continue
		}
		if !e.minutes[next.Minute()] {
			next = next.Add(time.Minute)
			continue
		}
		return next
	}
	return time.Time{}
}

// String returns the raw cron expression.
func (e *Expression) String() string {
	return e.raw
}
