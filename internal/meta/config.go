package meta

// Config defines metadata connection settings.
type Config struct {
	Endpoint        string
	RequireEndpoint bool
	AuthToken       string
	Enabled         bool
	DSN             string
}
