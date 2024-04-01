import os
import pandas as pd

# Access directory as an environment variable
optimizingdelivery_data_dir = os.environ.get('optimizingdelivery_data_dir')

# Define dataframe names
dataframe_names = ['customers', 'dates_2022', 'products', 'target_orders', 'order_lines', 'orders_aggregate']

for name in dataframe_names:
        file_path = os.path.join(optimizingdelivery_data_dir, f'{name}.parquet')
        try:
            globals()[f'{name}'] = pd.read_parquet(file_path)
            print(f"Dataframe '{name}' imported successfully.")
        except FileNotFoundError:
            print(f"Dataframe '{name}' import failed, file not found.")
        except Exception as e:
            print(f"Dataframe '{name}' import failed, an error occurred while importing: {e}")