import pandas as pd
import os

EPOCH = 3           # Numero di epoche su cui eseguire il test
DIM_FACTOR = .99    # Numero di record da replicare. Deve essere un numero nell'intervallo (0,1].

FILE_NAME = "historical_stock_prices.csv"
DATASET_PATH = "dataset/"

if DIM_FACTOR <= 0 and DIM_FACTOR > 1:
    raise ValueError("Il numero di campioni da replicare deve essere compreso nell'intervallo (0,1].")

if not os.path.exists(DATASET_PATH):
    os.makedirs(DATASET_PATH)

print(f"Reading {FILE_NAME}...")
all_data = pd.read_csv(FILE_NAME)

print("Starting the copy...")

all_data_length = all_data['ticker'].count()

for e in range(EPOCH):
    print(f"> {e+1} / {EPOCH}")
    if DIM_FACTOR==1:
        num_samples = all_data_length
        samples = all_data
    else:
        num_samples = all_data_length*DIM_FACTOR
        samples = all_data.sample(n=int(num_samples))
            
    # Aggiunta dei nuovi elementi al dataset
    print(f"Adding {int(num_samples)} new samples.")
    all_data = all_data.append(samples, ignore_index=True)
    all_data_length = all_data['ticker'].count()

    all_data.to_csv(f"{DATASET_PATH}{all_data_length}_{FILE_NAME}", index=False)
    print(f"{all_data_length}_{FILE_NAME} file created.")

print("Done.")
