# Data Taps - README

This folder contains scripts and configuration needed to extract external source data using the open source [Singer Taps](https://singer.io) platform.

## Table of Contents

1. [Table of Contents](#table-of-contents)
2. [Getting Started Guide](#getting-started-guide)
   1. [Step 1. Create `config.json` files with your secrets and configuration info](#step-1-create-configjson-files-with-your-secrets-and-configuration-info)
   2. [Step 2. Run `tapdance plan` and confirm the extraction plan](#step-2-run-tapdance-plan-and-confirm-the-extraction-plan)
   3. [Step 3. Run `tapdance sync` to test data extracts locally](#step-3-run-tapdance-sync-to-test-data-extracts-locally)

## Getting Started Guide

_The below examples use `tap-salesforce` and `tap-pardot` to extract data and `target-csv` to save the files locally in CSV data format._

### Step 1. Create `config.json` files with your secrets and configuration info

Create or modify your tap settings file(s) in the .secrets directory.

_Example Salesforce config `.secrets/tap-salesforce-config.json`:_

```json
{
    "api_type": "REST",
    "select_fields_by_default": true,
    "start_date": "2019-11-02T00:00:00Z",
    "username": "my.name@****.com.sfdc",
    "password": "**********",
    "security_token": "********",
    "disable_collection": false
}
```

_Example Pardot config `.secrets/tap-pardot-config.json`:_

```json
{
    "start_date": "2019-11-02T00:00:00Z",
    "email": "my.name@****.com",
    "password": "**********",
    "user_key": "********",
}
```

### Step 2. Run `tapdance plan` and confirm the extraction plan

```bash
# Use this script to create or update the metadata catalog for a specified tap.
# i.e.: tapdance plan TAP_NAME

tapdance plan pardot
tapdance plan salesforce
```

### Step 3. Run `tapdance sync` to test data extracts locally

```bash
# Salesforce extracts:
tapdance sync salesforce Account
tapdance sync salesforce Opportunity
tapdance sync salesforce OpportunityHistory
tapdance sync salesforce User

# Pardot extracts:
tapdance sync pardot *
```
