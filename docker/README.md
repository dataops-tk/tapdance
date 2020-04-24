# Singer images

This folder contains definitions to build the docker images as `slalomggp/singer`.

## Usage examples

Install the helper library:

```bash
pip3 install slalom.dataops
```

Build one or more docker images:

```bash
s-tap build_image tap-csv                 # Builds `slalomggp/singer:tap-csv`
s-tap build_image tap-csv --push          # Builds and pushes `slalomggp/singer:tap-csv`
s-tap build_image tap-csv target-redshift # Builds `slalomggp/singer:csv-to-redshift`
s-tap build_all_images --push             # Builds and pushes everything in the index
```
