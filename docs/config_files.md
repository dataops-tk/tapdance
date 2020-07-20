# Tapdance `config.json` Files

The Singer.io Tap and Target specifications provide that taps and targets can expect to
access settings and authorization credentials by means of a `config.json` file.

When run via command line, Tapdance expects that both of the following files exist,
relative to the working directory when tapdance is executed:

- .secrets/tap-{tap-id}-config.json
- .secrets/target-{target-id}-config.json

For example, if the current working directory is `c:\Files\my-project`, the tap is
`tap-covid-19`, and the target is `target-snowflake`, then Tapdance will look for the
files:

- `c:\Files\my-project\.secrets\covid-19.config\tap-covid-19.json`
- `c:\Files\my-project\.secrets\covid-19.config\target-snowflake.json`

Notes:

1. Tapdance will throw an error message if either file is not found.
2. To override the path for either config file, use the argument `--config_file` or
   `--target_config_file`.
3. To skip reading settings from a file altogether, use the argument `--config_file=False`
   or `--target_config_file=False`. This is useful when you are passing all settings via
   environment variables or if a target or tap does not require configuration.
