# Tapdance

Tapdance is an orchestration layer for the open source Singer tap platform.

## What Makes Tapdance is Different from other Singer and ETL Platforms

* **Tapdance focuses on ease-of-use** and is partially inspired by the ease-of-use of Pipelinewise.
* **Tapdance aims to support all taps** with no required plugin changes.
* **Tapdance is dockerized at the core** - we require that docker is installed, but we wrap it so you never have to run it directly.
* **Terraform has built-in IAC** (Infrastructure-As-Code) using Terraform and _near-zero_ infrastructure costs from always-on resources.
* **Tapdance is opinionated** in regards to ELT best practices.
* **Tapdance supports DevOps best practices** out of box, specifically: CI/CD and IAC.
* **Tapdance is platform-agnostic** and runs on Windows, Mac, and Linux alike using a _**docker-first**_ approach.
* **Tapdance plugins are curated** - when multiple forks exist for a given plugin, we will curate the best we find and use that as the default.
* **Tapdance uses a data-lake-first approach**; while it may be possible to load directly into an RDMS, we prioritize approaches where
* **Tapdance provides stream-isolation** - data can be extracted (and retried) one-table-at-a-time, even if this is not a feature of the plugin.
* **Tapdance automatically takes care of state** - state files are managed automatically for you.

## Installation

### Installation for Windows

1. [Install Chocolatey](https://chocolatey.org/docs/installation#install-with-cmdexe) from an _**Administrative**_ Command Prompt

    ```cmd
    @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command " [System.Net.ServicePointManager]::SecurityProtocol = 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"
    ```

2. Install Python3 (only if not installed)

    `choco install python3`

3. Install Docker

    `choco install -y docker`

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

Clone the repo and try a sample:

```bash
git clone https://github.com/aaronsteers/tapdance.git
```

Create a `rules` file:

`samples/taps/data.rules`

```ini
# Include all tables and all columns from all sources:

*.*.*

```

Select a sample:

* `cd samples/taps/covid-19-to-s3`
* `cd samples/taps/pardot-19-to-s3`
* `cd samples/taps/salesforce-19-to-s3`

Create the extract plan:

```bash
tapdance plan covid-19 --
```
