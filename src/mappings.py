"""
Entity and taxonomy mappings for data.gov.in datasets.

This module provides standardized mappings for:
- IMD meteorological subdivisions → Indian states
- Crop name variations → canonical crop names
- State name spelling variations → canonical state names
"""

# IMD Subdivision to State Mapping
# Source: India Meteorological Department subdivision boundaries
# Note: Some subdivisions span multiple states or are composite regions
SUBDIVISION_TO_STATE = {
    # Single-state mappings
    'Andaman & Nicobar Islands': 'Andaman and Nicobar Islands',
    'Arunanchal Pradesh': 'Arunachal Pradesh',  # Note: IMD spelling variation
    'Nagaland Manipur Mizoram Tripura': 'Nagaland',  # Composite subdivision
    'Sub Himalayan West Bengal & Sikkim': 'West Bengal',
    'Gangetic West Bengal': 'West Bengal',
    'Orissa': 'Odisha',  # Historical name
    'Jharkhand': 'Jharkhand',
    'Bihar': 'Bihar',
    'East Uttar Pradesh': 'Uttar Pradesh',
    'West Uttar Pradesh': 'Uttar Pradesh',
    'Uttarakhand': 'Uttarakhand',
    'Haryana Delhi & Chandigarh': 'Haryana',
    'Punjab': 'Punjab',
    'Himachal Pradesh': 'Himachal Pradesh',
    'Jammu & Kashmir': 'Jammu and Kashmir',
    'West Rajasthan': 'Rajasthan',
    'East Rajasthan': 'Rajasthan',
    'West Madhya Pradesh': 'Madhya Pradesh',
    'East Madhya Pradesh': 'Madhya Pradesh',
    'Gujarat Region': 'Gujarat',
    'Saurashtra & Kutch': 'Gujarat',
    'Konkan & Goa': 'Maharashtra',
    'Madhya Maharashtra': 'Maharashtra',
    'Marathwada': 'Maharashtra',
    'Vidarbha': 'Maharashtra',
    'Chhattisgarh': 'Chhattisgarh',
    'Coastal Andhra Pradesh': 'Andhra Pradesh',
    'Telangana': 'Telangana',
    'Rayalseema': 'Andhra Pradesh',
    'Tamil Nadu': 'Tamil Nadu',
    'Coastal Karnataka': 'Karnataka',
    'North Interior Karnataka': 'Karnataka',
    'South Interior Karnataka': 'Karnataka',
    'Kerala': 'Kerala',
    'Lakshadweep': 'Lakshadweep',
    # Composite regions (assign to primary state, note in comments)
    'Assam & Meghalaya': 'Assam',  # TODO: Split aggregation for Meghalaya
}

# Reverse mapping: State → Subdivisions (for aggregation)
STATE_TO_SUBDIVISIONS = {}
for subdiv, state in SUBDIVISION_TO_STATE.items():
    if state not in STATE_TO_SUBDIVISIONS:
        STATE_TO_SUBDIVISIONS[state] = []
    STATE_TO_SUBDIVISIONS[state].append(subdiv)

# State Name Canonical Mapping (handles spelling variations across datasets)
STATE_NAME_CANONICAL = {
    'Andaman and Nicobar Islands': ['Andaman & Nicobar Islands', 'A & N Islands'],
    'Andhra Pradesh': ['Andhra Pradesh', 'AP'],
    'Arunachal Pradesh': ['Arunachal Pradesh', 'Arunanchal Pradesh'],  # Common typo
    'Assam': ['Assam'],
    'Bihar': ['Bihar'],
    'Chhattisgarh': ['Chhattisgarh', 'Chattisgarh', 'Chhatisgarh'],
    'Goa': ['Goa'],
    'Gujarat': ['Gujarat', 'Gujrat'],
    'Haryana': ['Haryana'],
    'Himachal Pradesh': ['Himachal Pradesh', 'HP'],
    'Jharkhand': ['Jharkhand', 'Jarkhand'],
    'Karnataka': ['Karnataka', 'Karnatak'],
    'Kerala': ['Kerala'],
    'Madhya Pradesh': ['Madhya Pradesh', 'MP', 'M.P.'],
    'Maharashtra': ['Maharashtra', 'Maharastra'],
    'Manipur': ['Manipur'],
    'Meghalaya': ['Meghalaya'],
    'Mizoram': ['Mizoram'],
    'Nagaland': ['Nagaland'],
    'Odisha': ['Odisha', 'Orissa'],  # Historical name
    'Punjab': ['Punjab', 'Panjab'],
    'Rajasthan': ['Rajasthan'],
    'Sikkim': ['Sikkim'],
    'Tamil Nadu': ['Tamil Nadu', 'TN', 'Tamilnadu'],
    'Telangana': ['Telangana'],
    'Tripura': ['Tripura'],
    'Uttar Pradesh': ['Uttar Pradesh', 'UP', 'U.P.'],
    'Uttarakhand': ['Uttarakhand', 'Uttaranchal'],  # Historical name
    'West Bengal': ['West Bengal', 'WB', 'W.B.'],
    'Delhi': ['Delhi', 'NCT of Delhi', 'New Delhi'],
    'Chandigarh': ['Chandigarh'],
    'Puducherry': ['Puducherry', 'Pondicherry'],  # Historical name
    'Jammu and Kashmir': ['Jammu and Kashmir', 'Jammu & Kashmir', 'J&K'],
    'Ladakh': ['Ladakh'],
    'Dadra and Nagar Haveli and Daman and Diu': [
        'Dadra and Nagar Haveli and Daman and Diu',
        'Dadra & Nagar Haveli',
        'Daman & Diu'
    ],
    'Lakshadweep': ['Lakshadweep', 'Lakshadweep Islands'],
}

# Canonical Crop Names (Top 30 crops in Indian agriculture)
CROP_ALIASES = {
    # Cereals
    'Rice': ['Rice', 'Paddy', 'rice', 'paddy', 'RICE', 'PADDY', 'Dhan'],
    'Wheat': ['Wheat', 'wheat', 'WHEAT', 'Gehun'],
    'Maize': ['Maize', 'maize', 'MAIZE', 'Corn', 'corn', 'Makka'],
    'Jowar': ['Jowar', 'jowar', 'JOWAR', 'Sorghum', 'sorghum'],
    'Bajra': ['Bajra', 'bajra', 'BAJRA', 'Pearl Millet', 'pearl millet'],
    'Ragi': ['Ragi', 'ragi', 'RAGI', 'Finger Millet', 'finger millet'],
    'Barley': ['Barley', 'barley', 'BARLEY', 'Jau'],
    
    # Pulses
    'Arhar/Tur': ['Arhar', 'Tur', 'arhar', 'tur', 'ARHAR', 'TUR', 'Pigeon Pea', 'pigeon pea'],
    'Gram': ['Gram', 'gram', 'GRAM', 'Chana', 'chana', 'Chickpea'],
    'Moong': ['Moong', 'moong', 'MOONG', 'Green Gram', 'green gram', 'Mung'],
    'Urad': ['Urad', 'urad', 'URAD', 'Black Gram', 'black gram'],
    'Masoor': ['Masoor', 'masoor', 'MASOOR', 'Lentil', 'lentil'],
    
    # Oilseeds
    'Groundnut': ['Groundnut', 'groundnut', 'GROUNDNUT', 'Peanut', 'peanut', 'Mungfali'],
    'Rapeseed & Mustard': ['Rapeseed', 'Mustard', 'rapeseed', 'mustard', 'RAPESEED', 'MUSTARD', 'Sarson'],
    'Soybean': ['Soybean', 'soybean', 'SOYBEAN', 'Soyabean', 'Soya'],
    'Sunflower': ['Sunflower', 'sunflower', 'SUNFLOWER', 'Surajmukhi'],
    'Sesame': ['Sesame', 'sesame', 'SESAME', 'Sesamum', 'Til'],
    'Niger Seed': ['Niger Seed', 'niger seed', 'NIGER SEED', 'Nigerseed'],
    'Castor Seed': ['Castor Seed', 'castor seed', 'CASTOR SEED', 'Castor'],
    
    # Cash Crops
    'Sugarcane': ['Sugarcane', 'sugarcane', 'SUGARCANE', 'Sugar Cane', 'Ganna'],
    'Cotton': ['Cotton', 'cotton', 'COTTON', 'Kapas'],
    'Jute': ['Jute', 'jute', 'JUTE', 'Jute & Mesta'],
    'Tea': ['Tea', 'tea', 'TEA', 'Chai'],
    'Coffee': ['Coffee', 'coffee', 'COFFEE'],
    'Rubber': ['Rubber', 'rubber', 'RUBBER'],
    
    # Spices
    'Turmeric': ['Turmeric', 'turmeric', 'TURMERIC', 'Haldi'],
    'Coriander': ['Coriander', 'coriander', 'CORIANDER', 'Dhaniya'],
    'Chillies': ['Chillies', 'chillies', 'CHILLIES', 'Chilli', 'Chili', 'Mirchi'],
    'Ginger': ['Ginger', 'ginger', 'GINGER', 'Adrak'],
    
    # Horticulture
    'Potato': ['Potato', 'potato', 'POTATO', 'Aloo'],
    'Onion': ['Onion', 'onion', 'ONION', 'Pyaz'],
}

# Reverse mapping: Alias → Canonical
CROP_NAME_TO_CANONICAL = {}
for canonical, aliases in CROP_ALIASES.items():
    for alias in aliases:
        CROP_NAME_TO_CANONICAL[alias.lower()] = canonical


def normalize_state_name(state_raw: str) -> str:
    """
    Normalize state name to canonical form.
    
    Args:
        state_raw: Raw state name from dataset
        
    Returns:
        Canonical state name or original if no mapping found
    """
    state_clean = state_raw.strip()
    
    for canonical, aliases in STATE_NAME_CANONICAL.items():
        if state_clean in aliases:
            return canonical
    
    return state_clean  # Return original if no mapping


def normalize_crop_name(crop_raw: str) -> str:
    """
    Normalize crop name to canonical form.
    
    Args:
        crop_raw: Raw crop name from dataset
        
    Returns:
        Canonical crop name or original if no mapping found
    """
    crop_clean = crop_raw.strip().lower()
    return CROP_NAME_TO_CANONICAL.get(crop_clean, crop_raw)


def get_state_subdivisions(state_canonical: str) -> list:
    """
    Get all IMD subdivisions for a given state.
    
    Args:
        state_canonical: Canonical state name
        
    Returns:
        List of subdivision names
    """
    return STATE_TO_SUBDIVISIONS.get(state_canonical, [])
