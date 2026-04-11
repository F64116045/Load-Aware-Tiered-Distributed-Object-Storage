package main

import "testing"

func TestParseCPULoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		loadavg  string
		cpuCount int
		want     float64
	}{
		{
			name:     "normalized_load",
			loadavg:  "2.00 1.50 1.00 1/100 999",
			cpuCount: 4,
			want:     0.5,
		},
		{
			name:     "no_normalization_when_cpu_invalid",
			loadavg:  "3.25 1.20 0.80 1/100 999",
			cpuCount: 0,
			want:     3.25,
		},
		{
			name:     "bad_input",
			loadavg:  "oops",
			cpuCount: 8,
			want:     0,
		},
		{
			name:     "empty",
			loadavg:  "",
			cpuCount: 8,
			want:     0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := parseCPULoad(tt.loadavg, tt.cpuCount)
			if got != tt.want {
				t.Fatalf("parseCPULoad(%q, %d)=%v want=%v", tt.loadavg, tt.cpuCount, got, tt.want)
			}
		})
	}
}
