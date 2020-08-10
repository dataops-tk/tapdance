# Tapdance Tutorial

> This sample uses the covid-19 sample because it is very simple and only requires a github
  token for authorization.

**Clone the repo and try a sample:**

```bash
git clone https://github.com/aaronsteers/tapdance.git
```

**Create a `rules` file:**

Create or modify `samples/taps/covid-19/covid-19.rules.txt`:

```ini
# This is the simplest rules file, imports all tables and all columns:

*.*
```

**Create the extract plan:**

_The plan.yml file describes which columns and tables will be included, comparing the specified rules file with the tap's source schema. To create or update your plan file, run the following:_

```bash
tapdance plan covid-19
```

- For help, including a explanation of all optional parameters, run: `tapdance plan --help`

**Test the sync process locally:**

```bash
tapdance sync covid-19
```

- Tapdance will use [local CSV](https://github.com/singer-io/target-csv) as the default
  target if not other target is specified.
- For help, including a explanation of all optional parameters, run: `tapdance sync --help`

**Sync data to your S3 data lake:**

Once you've successfully tested the tap on your local machine, you are ready to load to
another target, such as S3 CSV.

```bash
tapdance sync covid-19 s3-csv
```

If you've not yet created a file called `.secrets/target-s3-csv-config.json`, the above
will fail and ask you to create this file first. Go ahead and create this file now using
the settings described in the [target-s3-csv](https://github.com/transferwise/pipelinewise-target-s3-csv#user-content-configuration-settings) documentation.
