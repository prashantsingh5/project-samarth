# Project Samarth — AI Coding Agent Instructions

## Project Overview
This is a **data.gov.in Q&A prototype** that fetches live agricultural and climate datasets, performs cross-domain reasoning, and returns cited answers. The system combines crop production data with rainfall patterns to answer analytical questions about India's agricultural economy.

**Goal**: End-to-end source-cited Q&A over live data.gov.in datasets (agriculture + climate).

## Architecture & Data Flow

### Core Components
1. **Data Sources** (`config/data_sources.json`): Metadata for 3 data.gov.in APIs
   - `agriculture_production`: District-level crop production (area, yield) by year/season
   - `agriculture_prices`: Daily commodity prices from Agmarknet mandis
   - `rainfall_subdiv_monthly`: IMD sub-division monthly rainfall (1901–2017)

2. **Normalizers** (`normalizers.py`): Standardization layer for heterogeneous API responses
   - `normalize_production()`: Maps raw production API → canonical schema (state, district, year, season, crop, area_ha, production_tonne)
   - `normalize_rainfall()`: Handles monthly/annual rainfall → (subdivision_name, year, rainfall_mm)
   - `aggregate_rainfall_to_state()`: Sub-division → state-level aggregation via mapping dicts

3. **Notebooks**: Exploratory development and testing
   - `test_endpoints.ipynb`: API connectivity validation (raw responses, field inspection)
   - `prototype.ipynb`: Integration testing for normalizers + data pipelines

### Key Design Decisions
- **Live data fetching**: No hardcoded CSVs; API-first with local caching planned
- **Privacy-first**: API key in `.env`, designed for self-hosted/offline deployment
- **Citation-driven**: Every metric links to dataset title, publisher, resource_id, filters

## Critical Workflows

### Environment Setup
```powershell
# Install dependencies
python -m pip install -r requirements.txt

# Configure API access (required for all data operations)
# 1. Copy .env.example to .env
# 2. Register at data.gov.in and add: DATA_GOV_IN_API_KEY=your_key_here
```

### Testing Data Pipelines
```python
# Always import normalizers from project root
import sys
sys.path.insert(0, '/c/Users/pytorch/Desktop/prototype')
from normalizers import normalize_production, normalize_rainfall

# Load config for resource IDs
import json
from pathlib import Path
config_path = Path('config/data_sources.json')
with open(config_path) as f:
    config = json.load(f)
```

### API Request Pattern (data.gov.in Resource API)
```python
import httpx
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv('DATA_GOV_IN_API_KEY')

url = f"https://api.data.gov.in/resource/{resource_id}"
params = {
    'api-key': api_key,
    'format': 'json',
    'filters[state_name]': 'Uttar Pradesh',  # Use dataset-specific filter syntax
    'limit': 100,
    'offset': 0
}
response = httpx.get(url, params=params, timeout=30)
records = response.json().get('records', [])
```

## Project-Specific Conventions

### Data Normalization Standards
- **Year handling**: Convert to `int` (Int64 for pandas), drop nulls; align calendar vs financial year explicitly
- **Unit harmonization**: 
  - Area: hectares (`area_ha`)
  - Production: tonnes (`production_tonne`)
  - Rainfall: millimeters (`rainfall_mm`)
- **Quality filters**: Drop rows with `production_tonne <= 0` or null; monthly rainfall summed across `['Jan', 'Feb', ..., 'Dec']`
- **String fields**: Always `.str.strip()` and fillna for state/district/crop names

### Schema Mapping Strategy
**Production schema** (7 columns):
```python
['state_name', 'district_name', 'year', 'season', 'crop', 'area_ha', 'production_tonne']
```

**Rainfall schema** (3 columns):
```python
['subdivision_name', 'year', 'rainfall_mm']
```

**Field name variations**: Normalizers handle aliases (e.g., `crop_year` → `year`, `area_` → `area_ha`, `subdivision`/`Subdivision` → `subdivision_name`)

### Integration Hurdles (from `proect.tx`)
- **Entity mapping**: State/district names vary across datasets → maintain mapping dictionaries (not yet implemented)
- **Crop taxonomy**: Same crop, different labels → canonical crop dimension needed
- **Time alignment**: Rainfall data ends at 2017; production data ongoing → always check year coverage
- **Geographic mismatch**: Rainfall is sub-division level, production is district level → aggregation function `aggregate_rainfall_to_state()` bridges this gap

## Dependencies & External Services

### Required Environment Variables
- `DATA_GOV_IN_API_KEY`: Mandatory for all API requests (free tier with rate limits)

### Key Libraries (from `requirements.txt`)
- **Data**: `pandas>=2.1`, `numpy>=1.26`, `scipy>=1.11` (correlations/stats)
- **HTTP**: `httpx>=0.26` (async-ready), `tenacity>=8.2` (retry logic)
- **UI**: `streamlit>=1.33` (planned frontend)
- **Validation**: `pydantic>=2.5` (schema enforcement)

### API Behavior Notes
- **Rate limits**: Free tier throttles; implement caching (`.cache/` directory exists but unused)
- **Filter syntax**: Use `filters[field_name]` for exact match, `filters[field.keyword]` for prices API
- **Pagination**: `offset`/`limit` params; `total` in response metadata
- **Timeouts**: Rainfall API can be slow → use `timeout=30` for httpx requests

## Cross-Domain Reasoning Patterns (from `prototype.ipynb`)

### Planned Query Types
1. **Rainfall comparison + top M crops** (states, last N years)
2. **District extremes** (highest/lowest production for crop X)
3. **Trend + correlation** (production vs rainfall over decade)
4. **Policy brief** (crop A vs B water intensity via rainfall/production proxy)

### Citation Tracking (design intent)
Every answer must attach:
- Dataset title, publisher
- Resource URL/ID
- Filters used
- Access date

## Development Notes

### Current Phase: Phase 4 (Normalizers & Mapping)
- ✅ Normalizers tested (`normalize_production`, `normalize_rainfall`)
- ✅ Validation functions (`validate_production_df`, `validate_rainfall_df`)
- ⏳ State-level aggregation mapping (subdivision → state dictionary not populated)
- ⏳ Canonical crop taxonomy (not started)
- ⏳ Caching layer (directory exists, implementation pending)

### Known Limitations
- No error handling for API failures beyond httpx timeout
- Rainfall data coverage ends at 2017 (IMD dataset limitation)
- No fuzzy matching for entity name variations
- Validation functions are boolean-only (no detailed error messages)

### File Naming Quirk
`proect.tx` (typo in filename) contains the full requirements/technical plan — treat as authoritative design doc.

## When Making Changes

### Adding New Normalizers
1. Follow existing pattern: accept `List[Dict]`, return `pd.DataFrame`
2. Include docstring with input/output schema and normalization steps
3. Add corresponding `validate_*_df()` function
4. Handle column name variations with fallback logic (see `normalize_rainfall` subdiv_col detection)

### Modifying Data Sources
Update `config/data_sources.json`:
- `resource_id`: Exact UUID from data.gov.in
- `fields_expected`: Document actual API field names for reference
- `sample_filters`: Include working filter examples (syntax varies by dataset)
- `notes`: Call out geometry mismatches, coverage gaps, or quirks

### Working with Notebooks
- Always use absolute paths in `sys.path.insert()` for imports (Windows paths with forward slashes)
- Load `.env` via `load_dotenv()` before accessing `DATA_GOV_IN_API_KEY`
- Use `httpx` (not `requests`) for consistency with async-ready codebase
