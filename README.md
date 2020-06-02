# Tapdance

Tapdance is an orchestration layer for the open source Singer tap platform.

## What Makes Tapdance Different from other Singer and ETL Platforms

_A bakers' dozen reasons to dance._

1. **Tapdance focuses on ease-of-use** and is partially inspired by the ease-of-use of Pipelinewise.
2. **Tapdance aims to support all taps** with no required plugin changes.
3. **Tapdance is dockerized at the core** - we require that docker is installed, but we wrap it so you never have to run it directly.
4. **Tapdance has built-in IAC** (Infrastructure-As-Code) using Terraform and _near-zero_ infrastructure costs from always-on resources.
5. **Tapdance is opinionated** in regards to ELT best practices.
6. **Tapdance supports DevOps best practices** out of box, specifically: CI/CD and IAC.
7. **Tapdance is platform-agnostic** and runs on Windows, Mac, and Linux alike using a _**docker-first**_ approach.
8. **Tapdance plugins are curated** - when multiple forks exist for a given plugin, we will curate the best we find and use those as the default. (See the latest list [here](docker/singer_index.yml).)
9. **Tapdance uses a data-lake-first approach** - while it may be possible to load directly into an RDMS, we prioritize approaches where data lands first in the data lake before ingestion into a SQL DW.
10. **Tapdance is rules-based** - instead of pointing and clicking a hundred times, simply tell tapdance what type of data you want (or what type of data you don't want).
11. **Tapdance knows your source schema is not static** and it adapts automatically in response to upstream schema changes.
12. **Tapdance provides stream-isolation** - data can be extracted (and retried) one-table-at-a-time, even if this is not a feature of the plugin.
13. **Tapdance automatically takes care of state** - state files are managed automatically for you so that incremental data loads come for free with no additional effort.

## Installation

### Installation for Windows

1. [Install Chocolatey](https://chocolatey.org/docs/installation#install-with-cmdexe) from an _**Administrative**_ Command Prompt

    ```cmd
    @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command " [System.Net.ServicePointManager]::SecurityProtocol = 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"
    ```

2. Install Python3 (if not already installed)

    `choco install -y python3`

3. Install Docker

    `choco install -y docker-desktop`

4. Install Tapdance

    `pip3 install tapdance`

### Installation for MacOS

1. [Install Homebrew](https://docs.brew.sh/Installation)
2. Install Python (only if not installed)

    `brew install python@3.8`

3. Install Docker

    `brew install docker`

4. Install Tapdance

    `pip3 install tapdance`

## Getting Started with a Sample

**Clone the repo and try a sample:**

```bash
git clone https://github.com/aaronsteers/tapdance.git
```

**Create a `rules` file:**

`samples/taps/covid-19/covid-19.rules.txt`

```ini
# This is the simplest rules file, imports all tables and all columns from all sources:

*.*
```

**Select a sample:**

_Pick from one of the below or create your own._

* `cd samples/taps/covid-19-to-s3`
* `cd samples/taps/pardot-19-to-s3`
* `cd samples/taps/salesforce-19-to-s3`

**Create the extract plan:**

_The plan.yml file describes which columns and tables will be included, comparing the specified rules file with the tap's source schema. To create or update your plan file, run the following:_

```bash
tapdance plan {my-tap-name}
```

**Test the sync process locally:**

```bash
tapdance sync {my-tap-name}
```

**Sync data to your S3 data lake:**

```bash
tapdance sync {my-tap-name} s3-csv
```
