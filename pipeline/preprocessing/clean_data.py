import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Data-Preprocessing")

def clean_f1_data(df, dataset_name):
    """
    Data Preprocessing Layer (Phase 15).
    Cleans data, removes nulls where appropriate, normalizes strings.
    """
    logger.info(f"Cleaning {dataset_name} dataset with shape {df.shape}")
    
    if dataset_name == 'drivers':
        # Replace \N with None
        df = df.replace(r'\\N', None, regex=True)
        # Normalize text
        if 'driverRef' in df.columns:
            df['driverRef'] = df['driverRef'].str.lower().str.strip()
        if 'code' in df.columns:
            df['code'] = df['code'].str.upper()
            
    elif dataset_name == 'results':
        df = df.replace(r'\\N', None, regex=True)
        # Ensure numerical types
        cols_to_numeric = ['points', 'laps', 'grid', 'position']
        for col in cols_to_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
    elif dataset_name == 'circuits':
        df = df.replace(r'\\N', None, regex=True)
        
    # Drop completely empty rows
    df = df.dropna(how='all')
    
    logger.info(f"Finished cleaning {dataset_name}. Final shape: {df.shape}")
    return df
