# Tapdance Environment Variables Guide

_Tapdance automatically scrapes a number of environment variables. This guide will explain
how to use environment variables within Tapdance._

## Setting Tap or Target settings via Environment Variable

To set configuration values for taps or targets, simply use the prefix the name of the
setting with `TAP_MYTAP_` for taps or `TARGET_MYTARGET_` for targets. If the tap or setting
name contains a dash (`-`), replace all dashes with underscores.

For example:

1. to set the `token_id` setting in the `covid-19` tap, you would set an
environment variable called `TAP_COVID_19_token_id`.
2. to set the `username` setting in the `target-snowflake` tap, you would set an
environment variable called `TARGET_SNOWFLAKE_username`.
