# TW_Housing_data_cleaning
Basic program for cleaning housing transaction data in Taiwan.


### `extract_zip.py`
Here's a revised version of your README for better clarity and structure:

---

### `extract_zip.py`

This script extracts files from a `.zip` archive.

#### Usage
To run the script in PowerShell, use the following command, assuming `$locA` and `$locB` are defined as the source and destination directories:

```powershell
python extract_zip.py $locA $locB 1
```

#### Arguments
- **`$locA`**: Path to the `.zip` file.
- **`$locB`**: Destination path where the file should be extracted.
- **`[0, 1]`**: Mode selection.
  - `0`: Extract Quarterly Data.
  - `1`: Extract Latest Data.


### RETR folder
This folder contains multiple functions designed to clean and preprocess housing data from the CSV files extracted by `extract_zip.py`.

#### `collect_rawdata.py`
