package meta

import "testing"

func TestResolveTiKVEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "plain_csv", in: "127.0.0.1:2379,127.0.0.1:2380", want: "127.0.0.1:2379,127.0.0.1:2380"},
		{name: "tikv_scheme", in: "tikv://127.0.0.1:2379", want: "127.0.0.1:2379"},
		{name: "memory_scheme", in: "memory://unit", want: "memory://unit"},
		{name: "empty", in: "", wantErr: true},
		{name: "scheme_only", in: "tikv://", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveTiKVEndpoints(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (got=%q)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveTiKVEndpoints(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNewRepository_RequiresDSNWithoutEndpoint(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(Config{
		Enabled: true,
	})
	if err == nil {
		t.Fatalf("expected error when tikv backend dsn is empty")
	}
}
