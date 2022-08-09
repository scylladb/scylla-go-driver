package frame

import "testing"

func TestDurationValidate(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		duration Duration
		valid    bool
	}{
		{
			name:     "zero",
			duration: Duration{},
			valid:    true,
		},
		{
			name: "positive",
			duration: Duration{
				Months:      1,
				Days:        2,
				Nanoseconds: 3,
			},
			valid: true,
		},
		{
			name: "positive with one zero",
			duration: Duration{
				Months:      1,
				Days:        0,
				Nanoseconds: 1,
			},
			valid: true,
		},
		{
			name: "positive with two zeros",
			duration: Duration{
				Months:      1,
				Days:        0,
				Nanoseconds: 0,
			},
			valid: true,
		},
		{
			name: "negative",
			duration: Duration{
				Months:      -1,
				Days:        -2,
				Nanoseconds: -3,
			},
			valid: true,
		},
		{
			name: "negative with one zero",
			duration: Duration{
				Months:      -1,
				Days:        0,
				Nanoseconds: -3,
			},
			valid: true,
		},
		{
			name: "negative with two zeros",
			duration: Duration{
				Months:      -1,
				Days:        0,
				Nanoseconds: 0,
			},
			valid: true,
		},
		{
			name: "mixed 1",
			duration: Duration{
				Months:      -1,
				Days:        -1,
				Nanoseconds: 1,
			},
			valid: false,
		},
		{
			name: "mixed 2",
			duration: Duration{
				Months:      -1,
				Days:        1,
				Nanoseconds: -1,
			},
			valid: false,
		},
		{
			name: "mixed 3",
			duration: Duration{
				Months:      1,
				Days:        -1,
				Nanoseconds: -1,
			},
			valid: false,
		},
		{
			name: "mixed 4",
			duration: Duration{
				Months:      -1,
				Days:        0,
				Nanoseconds: 1,
			},
			valid: false,
		},
		{
			name: "mixed 5",
			duration: Duration{
				Months:      -1,
				Days:        1,
				Nanoseconds: 0,
			},
			valid: false,
		},
		{
			name: "mixed 6",
			duration: Duration{
				Months:      0,
				Days:        -1,
				Nanoseconds: 1,
			},
			valid: false,
		},
		{
			name: "mixed 7",
			duration: Duration{
				Months:      0,
				Days:        1,
				Nanoseconds: -1,
			},
			valid: false,
		},
		{
			name: "mixed 8",
			duration: Duration{
				Months:      1,
				Days:        -1,
				Nanoseconds: 0,
			},
			valid: false,
		},
		{
			name: "mixed 9",
			duration: Duration{
				Months:      1,
				Days:        0,
				Nanoseconds: -1,
			},
			valid: false,
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.duration.validate()
			switch {
			case tc.valid && err != nil:
				t.Fatalf("expected no error, got %q", err)
			case !tc.valid && err == nil:
				t.Fatalf("expected error, got nil")
			}
		})
	}
}
