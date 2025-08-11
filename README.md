# ZST to JSON

Convert newline-delimited JSON compressed with Zstandard (.zst) to plain JSON lines using a streaming, memory-efficient approach.

## What this script does
- Scans the script's directory for all files ending in `.zst`.
- Decompresses each file using streaming to keep memory usage low.
- Validates each line as JSON, writing only valid lines to a `.json` file with the same basename in the same directory.
- Prints periodic progress and a summary of valid/invalid lines.

## Why streaming?
Many `.zst` archives contain very large newline-delimited JSON datasets. Streaming avoids loading the entire file into memory, making it suitable for very large inputs.

## Requirements
- Python 3.8+
- zstandard library

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage
1. Place your `.zst` files in the same folder as `zst_to_json.py`.
2. Run the script:

```bash
python zst_to_json.py
```

3. Output `.json` files will be written alongside the input `.zst` files.

### Notes
- Only valid JSON lines are written to output. Malformed lines are counted and skipped.
- The script prints a progress message every 10,000 valid lines processed.
- If no `.zst` files are found in the script directory, the script will print a helpful message and exit.

## License
Licensed under the Apache License, Version 2.0. See `LICENSE` for details.
