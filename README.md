# Project Samarth 🌾

**An intelligent Q&A system for Indian agricultural and climate data**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Data Source](https://img.shields.io/badge/data-data.gov.in-green.svg)](https://data.gov.in)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

Project Samarth is a production-ready Q&A system that fetches live agricultural and climate datasets from [data.gov.in](https://data.gov.in), performs cross-domain reasoning, and returns cited answers with supporting data. The system combines crop production data with rainfall patterns to answer analytical questions about India's agricultural economy.

**Key Features:**
- 🤖 Natural language question parsing using Google Gemini AI
- 📊 Real-time data from 3 government datasets (1997-present)
- 🔍 Cross-domain analysis (production + rainfall correlation)
- 📝 Source citations for every answer
- ⚡ Intelligent caching with 24-hour TTL
- 🌐 Entity normalization (states, crops, districts)

## Architecture

```
┌─────────────────┐
│  User Question  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Query Planner (Gemini AI)      │
│  - Intent detection             │
│  - Entity extraction            │
│  - Query validation             │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Query Executor                 │
│  - Data fetching                │
│  - Cross-domain analysis        │
│  - Result formatting            │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Answer + Data + Citations      │
└─────────────────────────────────┘
```

## Supported Query Types

1. **Comparison Queries** 
   - *"Which state had more rice production in 2015 - Karnataka or Tamil Nadu?"*
   
2. **Extremes Queries**
   - *"Which district in Karnataka has the highest maize production?"*
   
3. **Trends Queries**
   - *"Show me the rainfall trend in Kerala from 2010 to 2015"*
   
4. **Correlation Queries**
   - *"Is there a correlation between rainfall and rice yield in Andhra Pradesh?"*

## Data Sources

All data is fetched live from [data.gov.in](https://data.gov.in) APIs:

| Dataset | Coverage | Records | Update Frequency |
|---------|----------|---------|------------------|
| **Crop Production** | 1997-present | 1M+ | Annual |
| **Rainfall (IMD)** | 1901-2017 | 4K+ | Historical |
| **Commodity Prices** | 2016-present | 100K+ | Daily |

## Installation

### Prerequisites
- Python 3.11 or higher
- Google Gemini API key ([Get one free](https://aistudio.google.com/app/apikey))
- Data.gov.in API key ([Register here](https://data.gov.in/user/register))

### Setup

1. **Clone the repository**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
4. **Configure API keys**
```bash
cp .env.example .env
# Edit .env and add your API keys:
# DATA_GOV_IN_API_KEY=your_data_gov_in_key
### Quick Start with Jupyter Notebook

1. **Launch Jupyter**
jupyter notebook notebooks/demo.ipynb
```
2. **Run all cells** to see the complete pipeline in action

### Python API Usage
```python
import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Initialize components
connector = DataGovInConnector(
    api_key=os.getenv('DATA_GOV_IN_API_KEY'),
executor = QueryExecutor(connector)

# Ask a question
question = "Which state had more rice production in 2015 - Karnataka or Tamil Nadu?"

result = executor.execute(plan)

# Display answer
print(result.answer)
print(result.data)
print(result.metadata)
```

## Project Structure

```
prototype/
├── src/                          # Core source code
│   ├── __init__.py              # Package initialization
│   ├── query_planner.py         # Gemini AI query parser
│   ├── query_executor.py        # Query execution engine
│   ├── data_connector.py        # Data.gov.in API connector
│   ├── normalizers.py           # Data normalization functions
│   ├── mappings.py              # Entity mapping (states, crops)
│   └── cache.py                 # Caching layer (24h TTL)
├── config/
│   └── demo.ipynb               # Interactive demo & testing
├── .cache/                      # Auto-generated cache directory
├── .env                         # API keys (not in git)
```

## How It Works
2. **Normalization**: Standardize heterogeneous data schemas
3. **Entity Mapping**: Normalize state/crop names across datasets
4. **Caching**: Local cache with 24-hour TTL to respect rate limits

### Phase 2: Intelligent Q&A

## Key Features

### 🔄 Data Normalization
- Converts heterogeneous API responses to canonical schemas
- Unit harmonization (hectares, tonnes, millimeters)
- Quality filters (drops null/invalid records)


### ⚡ Performance
- **Caching**: 24-hour TTL reduces API calls by ~80%
### 📊 Analytics
- **Aggregation**: Sum, average, max, min by state/district/year
- **Trends**: Time-series analysis with percentage changes
- **Correlation**: Pearson correlation coefficient (production vs rainfall)
- **Extremes**: Identify highest/lowest producing districts

## Configuration

### API Rate Limits
- **Data.gov.in**: Free tier with throttling
- **Gemini AI**: Free tier (60 requests/minute)
- **Cache**: 24-hour TTL (configurable in `cache.py`)

### Customization
- **Add new datasets**: Update `config/data_sources.json`
- **Modify cache TTL**: Edit `CacheManager.__init__()` in `src/cache.py`
- **Add crop aliases**: Update `CROP_ALIASES` in `src/mappings.py`
- **Change AI model**: Update model name in `src/query_planner.py` (currently `gemini-2.5-flash`)

## Troubleshooting

### Common Issues

**1. API Key Invalid**
```bash
# Test your Gemini key
python -c "import google.generativeai as genai; genai.configure(api_key='YOUR_KEY'); print(list(genai.list_models())[:1])"
```

**2. Module Import Errors**
```bash
# Ensure you're in the project root
cd /path/to/prototype
python -c "from src import QueryPlanner"
```

**3. Cache Permission Errors**
```bash
# Check .cache/ directory permissions
ls -la .cache/
```

**4. Empty Results**
- Check year coverage (rainfall ends at 2017)
- Verify state/crop names match mappings
- Review filters in `config/data_sources.json`

## Development

### Running Tests
```bash
# Test API connectivity
python -c "from src import DataGovInConnector; print('OK')"

# Test normalizers
python -c "from src import normalize_production; print('OK')"

# Test end-to-end in notebook
jupyter notebook notebooks/demo.ipynb
```

### Adding New Query Types
1. Add intent to `QueryPlan` dataclass in `src/query_planner.py`
2. Add execution method in `src/query_executor.py`
3. Update Gemini prompt to recognize new intent
4. Add test cases in `notebooks/demo.ipynb`

## Limitations

- **Rainfall data**: Ends at 2017 (IMD dataset limitation)
- **Entity matching**: No fuzzy string matching yet
- **Geographic mismatch**: Rainfall (subdivision-level) vs Production (district-level)
- **API throttling**: Free tier rate limits apply

## Future Enhancements

- [ ] Streamlit web UI
- [ ] Visualization (charts/maps)
- [ ] Multi-year comparisons
- [ ] Export results (CSV/JSON/PDF)
- [ ] Fuzzy entity matching
- [ ] Support for price data queries
- [ ] Historical trend predictions

## License

MIT License - See LICENSE file for details

## Acknowledgments

- **Data Source**: [data.gov.in](https://data.gov.in) (Government of India)
- **AI Model**: Google Gemini 2.5 Flash
- **Weather Data**: India Meteorological Department (IMD)

## Contact

For questions or feedback, please open an issue on GitHub.

---

**Built for the Fellowship Challenge** | **Version 1.0.0** | **October 2025**
