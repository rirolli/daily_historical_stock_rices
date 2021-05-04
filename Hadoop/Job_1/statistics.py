#!/usr/bin/env python3

import subprocess
import random
import time
import os
import matplotlib

import pandas as pd
import numpy as np
import matplotlib.pylab as plt

from pathlib import Path

EPOCH = 1  # Numero di epoche su cui eseguire il test
DIM_FACTOR = .0025

def str_time_prop(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def random_valid_date(start, end, list_of_dates):
    """ Funzione che genera una data nuova che non fa parte dell'elenco delle date
    già presenti nel dataset
    """
    valid_date = False
    while not valid_date:
        date = str_time_prop(start, end, '%Y-%m-%d', random.random())
        if date not in list_of_dates:
            valid_date = True
    return date

def get_values_from_dataset(data):
    max_values = data.max(axis=0)
    min_values = data.min(axis=0)

    ticker_list = (data['ticker'].unique()).tolist()
    old_date_list = (data['date'].unique()).tolist()

    open_a_max_min = (max_values.get('open'),min_values.get('open'))
    close_a_max_min = (max_values.get('close'),min_values.get('close'))
    adj_close_max_min = (max_values.get('adj_close'),min_values.get('adj_close'))
    low_max_min = (max_values.get('low'),min_values.get('low'))
    high_max_min = (max_values.get('high'),min_values.get('high'))
    volume_max_min = (max_values.get('volume'),min_values.get('volume'))
    date_max_min = (max_values.get('date'),min_values.get('date'))

    return ticker_list, old_date_list, open_a_max_min, close_a_max_min, adj_close_max_min, low_max_min, high_max_min, volume_max_min, date_max_min

def plot_history(history):
    savefig_url = f"plot/"
    if not os.path.exists(os.path.dirname(savefig_url)):
        try:
            Path(savefig_url).mkdir(parents=True, exist_ok=True)
            print(f"Folder {savefig_url} created")
        except:
            raise OSError(f"Creation of the directory {savefig_url} failed")

    lists = sorted(history.items()) # sorted by key, return a list of tuples

    x, y = zip(*lists) # unpack a list of pairs into two tuples

    plt.title('Tempo di processione')
    plt.ylabel('Tempo')
    plt.xlabel('#Dati')
    plt.plot(x, y)
    fig = plt.gcf()
    fig.savefig(savefig_url+"datatime.png", dpi=600)

# all_data.sample(frac=.25)

# subprocess.run(["ls"])
# subprocess.run(["./run.sh"])

def main():
    all_data = pd.read_csv("historical_stock_prices.csv")

    all_data_length = all_data['ticker'].count()

    ticker_list, old_date_list, open_a_max_min, close_a_max_min, adj_close_max_min, low_max_min, high_max_min, volume_max_min, date_max_min = get_values_from_dataset(all_data)

    new_date = random_valid_date(date_max_min[0], "2999-12-31", old_date_list)
    old_date_list.append(new_date)

    stat = {}

    for e in range(EPOCH):
        # Eseguo Hadoop
        start_t = time.time()
        subprocess.run(["./run.sh"])
        stop_t = time.time()
        delta_time = stop_t-start_t
        
        stat[all_data_length] = delta_time

        if not(e==(EPOCH-1)):
            # Generazione di nuovi esempi da aggiungere al dataset
            samples = all_data.sample(n=int(all_data_length*DIM_FACTOR))
                    
            # Aggiunta dei nuovi elementi al dataset
            all_data = all_data.append(samples, ignore_index=True)
            all_data_length = all_data['ticker'].count()

            samples.to_csv("data.csv", mode='a', index=False)

    stat_df = pd.DataFrame(stat.items(), columns=['data_dimension', 'time'])
    stat_df.to_csv("stats.csv", index=False)
    plot_history(stat)
    

if __name__ == "__main__":
    main()