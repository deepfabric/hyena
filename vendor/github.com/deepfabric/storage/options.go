package storage

// Option is prophet create option
type Option func(*options)

type options struct {
	nemoDataPath string
	nemoOptions  string
}

// WithNemoOptions using nemo configuration file
func WithNemoOptions(nemoOptions string) Option {
	return func(opts *options) {
		opts.nemoOptions = nemoOptions
	}
}

// WithNemoDataPath using nemo data path
func WithNemoDataPath(nemoDataPath string) Option {
	return func(opts *options) {
		opts.nemoDataPath = nemoDataPath
	}
}
