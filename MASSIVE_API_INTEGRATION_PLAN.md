# Massive API Integration Plan - Theoretical Approach

## Current Data Structure Analysis

### Existing CSV Format
- **Ticker Format**: `O:TSLA10219C006667000`
  - `O:` = Option prefix
  - `TSLA` = Underlying symbol
  - `10219` = Expiration date (YYMMDD = 2021-02-19)
  - `C` = Option type (C=Call, P=Put)
  - `006667000` = Strike price encoded (66.67 * 100000 = 6667000)
  
### Current Columns (29 total)
1. `ticker` - Option identifier
2. `date_only` - Trading date
3. `expiration_date` - Option expiration
4. `underlying_symbol` - Stock symbol (TSLA)
5. `option_type` - C or P
6. `strike` - Strike price (decimal)
7. `volume` - Trading volume
8. `open_price` - Opening price
9. `close_price` - Closing price
10. `otm_pct` - Out-of-the-money percentage
11. `ITM` - In-the-money flag (YES/NO)
12. `premium` - Option premium
13. `premium_yield_pct` - Premium yield percentage
14. `premium_low` - Low premium
15. `premium_yield_pct_low` - Low premium yield
16. `high_price` - High price
17. `low_price` - Low price
18. `transactions` - Number of transactions
19. `window_start` - Timestamp (nanoseconds)
20. `days_to_expiry` - Days until expiration
21. `time_remaining_category` - Monthly/Weekly
22-29. Underlying price columns (open, close, high, low, spot, at_expiry variants)

---

## Massive API Endpoint Analysis

### Endpoint Structure
```
https://api.massive.com/v3/snapshot/options/{exchange}/{ticker}?apiKey={key}
```

**Example**: `https://api.massive.com/v3/snapshot/options/A/O:A250815C00055000?apiKey=7asm1ymlJwuul2I4LWFzsvVtuWDUd3Q0`

### Components:
- **Base URL**: `https://api.massive.com/v3/snapshot/options/`
- **Exchange**: `A` (likely AMEX/American exchange)
- **Ticker Format**: `O:A250815C00055000`
  - Similar to our format but with `A` prefix instead of symbol
  - `A250815` = Expiration (2025-08-15)
  - `C` = Call
  - `00055000` = Strike (550.00)
- **API Key**: `7asm1ymlJwuul2I4LWFzsvVtuWDUd3Q0`

### Expected API Response (Unknown - needs testing)
We need to understand:
- What fields does the API return?
- What's the data structure?
- Does it return historical snapshots or current only?
- Rate limits?
- Date range parameters?

---

## Integration Strategy

### Phase 1: Data Discovery & Mapping

#### Step 1.1: Extract Unique Tickers from Existing Data
- Read TSLA 2021 monthly CSV
- Extract unique `ticker` values
- Count total unique contracts
- **Expected**: ~thousands of unique option contracts

#### Step 1.2: Convert Ticker Format
- **Current Format**: `O:TSLA10219C006667000`
- **API Format**: `O:A250815C00055000`
- **Conversion Logic**:
  - Remove `O:` prefix
  - Extract: symbol, expiration (YYMMDD), type, strike
  - Convert to API format: `O:{exchange}{YYMMDD}{type}{strike_encoded}`
  - **Question**: What exchange code for TSLA? (likely `A` or `Q` for NASDAQ)

#### Step 1.3: Test API Call
- Make single test request
- Understand response structure
- Identify available fields
- Check for rate limits

### Phase 2: Data Fetching Strategy

#### Step 2.1: Batch Processing
- Group tickers by expiration date
- Process in batches to respect rate limits
- Implement retry logic for failed requests

#### Step 2.2: Date Range Handling
- Current data has `date_only` (trading dates)
- API might need date parameters
- May need to fetch multiple snapshots per ticker (one per trading day)

#### Step 2.3: Error Handling
- Handle missing contracts (expired, delisted)
- Handle API errors (rate limits, timeouts)
- Log failures for manual review

### Phase 3: Data Merging Strategy

#### Step 3.1: Field Mapping
- Map API response fields to existing columns
- Identify new fields from API not in current data
- Handle missing fields (use existing data as fallback)

#### Step 3.2: Merge Logic
- **Primary Keys**: `ticker` + `date_only`
- **Merge Strategy**: 
  - Left join: Keep all existing records
  - Update with API data where available
  - Add new fields from API as additional columns

#### Step 3.3: Data Validation
- Verify strike prices match
- Verify expiration dates match
- Check for data inconsistencies
- Validate numeric ranges

### Phase 4: Implementation Considerations

#### 4.1: Rate Limiting
- Implement delays between requests
- Batch requests if API supports it
- Cache responses to avoid duplicate calls

#### 4.2: Data Storage
- Store raw API responses (JSON) for debugging
- Create intermediate CSV files per batch
- Final merged output

#### 4.3: Progress Tracking
- Log progress (X of Y tickers processed)
- Save checkpoint files
- Resume capability if interrupted

---

## Key Questions to Resolve

1. **Exchange Code**: What exchange code should we use for TSLA?
   - `A` = AMEX? ❌ Tested - not found
   - `Q` = NASDAQ? ❌ Tested - not found
   - `X` = NYSE? ❌ Tested - not found
   - **Issue**: Contracts from 2021 not found - API may not have historical data

2. **API Response Structure**: What does the API actually return?
   - Fields available? **Unknown - need working example**
   - Historical data or current snapshot only? **Likely current only**
   - Date range parameters? **Need to check API docs**

3. **Ticker Format Conversion**: 
   - Our format: `O:TSLA10219C006667000`
   - API format: `O:A250815C00055000`
   - **Alternative**: Maybe API needs symbol in ticker? `O:TSLA10219C006667000`?
   - **Or**: Different endpoint structure?

4. **Date Handling**:
   - Does API support historical snapshots? **Unlikely - 2021 contracts not found**
   - Do we need date parameters in URL? **Need to check**
   - Or fetch current snapshot only? **Most likely**

5. **Rate Limits**:
   - Requests per second/minute? **Unknown**
   - Daily limits? **Unknown**
   - Need API key rotation? **Unknown**

6. **Data Completeness**:
   - Will API have all contracts we have? **Unlikely - expired contracts missing**
   - What about expired/delisted options? **Probably not available**
   - How to handle missing data? **Use existing data as fallback**

## ⚠️ CRITICAL FINDINGS FROM TESTING

### Test Results:
- Tested exchanges: A, Q, X
- All returned: `"Options contract not found"`
- Tested ticker: `O:TSLA10219C006667000` (from 2021-02-19)
- **Conclusion**: API likely doesn't have historical/expired contracts

### Possible Solutions:

1. **Try Current/Future Contracts Only**:
   - Test with contracts expiring in 2025 or later
   - API might only have active contracts

2. **Different Endpoint**:
   - Maybe there's a historical endpoint?
   - Or bulk/chain endpoint?

3. **Different Ticker Format**:
   - Try keeping symbol: `O:TSLA10219C006667000` instead of `O:A10219C006667000`
   - Or different encoding?

4. **Contact Massive Support**:
   - Ask about historical data availability
   - Ask about correct ticker format
   - Ask about endpoint structure

---

## Proposed Implementation Flow

```
1. Load existing TSLA 2021 data
   ↓
2. Extract unique tickers
   ↓
3. Convert ticker format for API
   ↓
4. Test single API call
   ↓
5. Analyze response structure
   ↓
6. Implement batch fetching with rate limiting
   ↓
7. Store raw API responses
   ↓
8. Parse and normalize API data
   ↓
9. Merge with existing data
   ↓
10. Validate merged data
   ↓
11. Save final merged CSV
```

---

## Next Steps

1. **Test API Endpoint**: Make a single test call to understand response
2. **Document Response**: Record all fields returned
3. **Create Ticker Converter**: Function to convert our format to API format
4. **Implement Fetcher**: Batch processing with error handling
5. **Implement Merger**: Join logic with validation
6. **Test on Small Subset**: Process 10-20 tickers first
7. **Scale Up**: Process all TSLA 2021 tickers

---

## Files to Create

1. `fetch_massive_data.py` - Main fetching script
2. `ticker_converter.py` - Format conversion utilities
3. `api_client.py` - API interaction wrapper
4. `merge_massive_data.py` - Data merging logic
5. `config.py` - API keys and configuration

---

## Risk Considerations

1. **API Changes**: API structure might change
2. **Rate Limits**: May need to throttle requests
3. **Data Mismatches**: API data might not match existing data
4. **Missing Contracts**: Some contracts might not exist in API
5. **Cost**: Check if API has usage costs
6. **Data Quality**: Validate API data matches expectations

