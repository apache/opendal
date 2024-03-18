package config

type Config struct {
	Name string `toml:"name"`
	Doc  string `toml:"doc"`
}

type ConfigField struct {
	Name string `toml:"name"`
	Doc  string `toml:"doc"`
}
