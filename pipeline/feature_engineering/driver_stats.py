import pandas as pd
import numpy as np

def engineer_driver_features(results_df, driver_df):
    """
    Performs Feature Engineering on driver data (Phase 16).
    Calculates:
    - career_points
    - win_rate
    - total_podiums
    """
    # Group results by driver
    driver_stats = results_df.groupby('driver_id').agg(
        career_points=('points', 'sum'),
        total_races=('race_id', 'nunique'),
        wins=('finish_position', lambda x: (x == 1).sum()),
        total_podiums=('finish_position', lambda x: (x <= 3).sum())
    ).reset_index()
    
    # Calculate win rate
    driver_stats['win_rate'] = (driver_stats['wins'] / driver_stats['total_races'] * 100).round(2)
    
    # Merge with driver details
    enhanced_driver_df = pd.merge(driver_df, driver_stats, on='driver_id', how='left')
    
    # Fill NaN values for rookies / drivers with no races
    enhanced_driver_df.fillna({
        'career_points': 0.0,
        'total_races': 0,
        'wins': 0,
        'total_podiums': 0,
        'win_rate': 0.0
    }, inplace=True)
    
    return enhanced_driver_df

if __name__ == "__main__":
    # Small test
    dummy_results = pd.DataFrame({
        'driver_id': ['hamilton', 'hamilton', 'bottas'],
        'race_id': [1, 2, 1],
        'finish_position': [1, 2, 3],
        'points': [25, 18, 15]
    })
    dummy_drivers = pd.DataFrame({
        'driver_id': ['hamilton', 'bottas'],
        'driver_name': ['Lewis Hamilton', 'Valtteri Bottas']
    })
    
    result = engineer_driver_features(dummy_results, dummy_drivers)
    print(result)
