package cronexpr

import (
	"testing"
	"time"
)

func TestParseAndMatches(t *testing.T) {
	tests := []struct {
		expr    string
		t       time.Time
		matches bool
	}{
		{
			expr:    "* * * * *",
			t:       time.Date(2026, 10, 22, 14, 30, 0, 0, time.UTC),
			matches: true,
		},
		{
			expr:    "*/10 * * * *",
			t:       time.Date(2026, 10, 22, 14, 30, 0, 0, time.UTC),
			matches: true,
		},
		{
			expr:    "*/10 * * * *",
			t:       time.Date(2026, 10, 22, 14, 35, 0, 0, time.UTC),
			matches: false,
		},
		{
			expr:    "0 3 * * *",
			t:       time.Date(2026, 10, 22, 3, 0, 0, 0, time.UTC),
			matches: true,
		},
		{
			expr:    "0 3 * * *",
			t:       time.Date(2026, 10, 22, 4, 0, 0, 0, time.UTC),
			matches: false,
		},
		{
			expr:    "15,45 8-12 * * 1-5", // Mon-Fri, 8-12 at 15 and 45 mins
			t:       time.Date(2026, 10, 22, 10, 45, 0, 0, time.UTC), // 2026-10-22 is Thursday (dow 4)
			matches: true,
		},
	}

	for _, tc := range tests {
		e, err := Parse(tc.expr)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tc.expr, err)
		}
		got := e.Matches(tc.t)
		if got != tc.matches {
			t.Errorf("expr '%s' matching %v: expected %v, got %v", tc.expr, tc.t, tc.matches, got)
		}
	}
}

func TestNext(t *testing.T) {
	e, err := Parse("*/15 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	from := time.Date(2026, 10, 22, 14, 10, 20, 0, time.UTC)
	next := e.Next(from)
	expected := time.Date(2026, 10, 22, 14, 15, 0, 0, time.UTC)

	if !next.Equal(expected) {
		t.Fatalf("Expected next run %v, got %v", expected, next)
	}
}
