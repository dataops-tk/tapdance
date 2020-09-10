# Singer images

This folder contains definitions to build the docker images as `dataopstk/tapdance`.

## Usage examples

Install the helper library:

```bash
pip3 install tapdance
```

Build one or more docker images:

```bash
tapdance build_image csv                # Builds `dataopstk/tapdance:tap-csv`
tapdance build_image csv --push         # Builds and pushes `dataopstk/tapdance:tap-csv`
tapdance build_image csv redshift       # Builds `dataopstk/tapdance:csv-to-redshift`
tapdance build_all_images --push        # Builds and pushes everything in the index
```
