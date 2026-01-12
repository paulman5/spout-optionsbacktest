# 🚀 Using Local Files (MUCH FASTER!)

## Why Use Local Files?

If your friend downloads the files from Google Drive/Dropbox to their computer, the script can read from those local files instead of downloading from S3. This is **10-100x FASTER** because:

- ✅ **No network latency** (Netherlands → US = 100-200ms per request)
- ✅ **No internet needed** after files are downloaded
- ✅ **Much faster processing** (30 seconds vs 15-25 minutes per year!)

## How to Set It Up

### Step 1: Download Files from Drive/Dropbox

Your friend needs to download the files and organize them like this:

```
downloaded_data/
├── 2016/
│   ├── 01/
│   │   ├── 2016-01-04.csv.gz
│   │   ├── 2016-01-05.csv.gz
│   │   └── ...
│   ├── 02/
│   │   └── ...
│   └── ...
├── 2017/
│   └── ...
└── 2018/
    └── ...
```

**OR** any structure where files are organized by year/month.

### Step 2: Update the Script

Open `aggregate.py` and find these lines (around line 131-133):

```python
USE_LOCAL_FILES = False  # ← Set to True if files are downloaded locally
LOCAL_FILES_PATH = "./downloaded_data"  # ← Path to folder with downloaded files
```

**Change to:**

```python
USE_LOCAL_FILES = True  # ← Changed to True!
LOCAL_FILES_PATH = "./downloaded_data"  # ← Change this to where your files are
```

**If files are in a different location, update the path:**
- `LOCAL_FILES_PATH = "./data"`  (if in a "data" folder)
- `LOCAL_FILES_PATH = "/Users/YourName/Downloads/options_data"`  (full path)
- `LOCAL_FILES_PATH = "C:\\Users\\YourName\\Downloads\\options_data"`  (Windows path)

### Step 3: Run the Script

That's it! Just run the script normally:

```bash
cd python-boilerplate
python3 -u src/backtesting/data/aggregate.py
```

The script will automatically:
- ✅ Detect that you're using local files
- ✅ Skip S3 setup (no internet needed!)
- ✅ Read from your local files (super fast!)
- ✅ Process everything in 30 seconds to 2 minutes (instead of 15-25 minutes!)

## Expected Speed

| Method | Time per Year | Notes |
|--------|---------------|-------|
| **Local Files** | **30 sec - 2 min** | ⚡ Super fast! |
| S3 (US user) | 3-5 minutes | Good, but slower |
| S3 (Netherlands) | 15-25 minutes | Slow due to latency |

## File Structure Options

The script will automatically try to find files in these patterns:

1. `LOCAL_FILES_PATH/2017/*/*.csv.gz` (year/month/files)
2. `LOCAL_FILES_PATH/*/*/*.csv.gz` (any structure)
3. `LOCAL_FILES_PATH/*2017*/*/*.csv.gz` (year in folder name)

So your friend doesn't need perfect organization - the script will find the files!

## Troubleshooting

### "No local files found"
- Check that `LOCAL_FILES_PATH` points to the right folder
- Make sure files are `.csv.gz` format
- Try using the full path instead of relative path

### "Path does not exist"
- Make sure the folder exists
- Check spelling of the path
- On Windows, use `\\` instead of `/` or use forward slashes `/`

### Still slow?
- Make sure `USE_LOCAL_FILES = True` (not `False`)
- Check that files are actually on local disk (not network drive)
- SSD is faster than HDD, but both work

## Summary

**For your friend:**
1. Download files from Drive/Dropbox
2. Set `USE_LOCAL_FILES = True`
3. Set `LOCAL_FILES_PATH` to the folder location
4. Run script - it's now 10-100x faster! 🚀











