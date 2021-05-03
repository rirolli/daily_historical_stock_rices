#!/usr/bin/env python3
"""reducer.py"""

# per ciascuna azione:
# (a) la data della prima quotazione,
# (b) la data dell’ultima quotazione,
# (c) la variazione percentuale della quotazione
#   (differenza percentuale tra il primo e l’ultimo
#   prezzo di chiusura presente nell’archivio),
# (d) il prezzo massimo e quello minimo
# (e) (facoltativo) il massimo numero di giorni
#   consecutivi in cui l’azione è cresciuta (chiusura
#   maggiore dell’apertura) con indicazione dell’anno
#   in cui questo è avvenuto.
# 
# Il report deve essere ordinato per valori decrescenti del punto b.

import sys

from datetime import datetime

# Funzioni di supporto
def percentage_change(i,f):
    r = 100*((f-i)/i)
    return r

def convert_input_data(ticker, close_a, low, high, date):
    close_a = float(close_a)
    low = float(low)
    high = float(high)
    date = datetime.strptime(date, '%Y-%m-%d')

    return ticker, close_a, low, high, date


# Blocco di esecuzione del reducer.py
action_map = {}

for line in sys.stdin:
    # Gestione e conversione dei dati in input

    line = line.strip()

    ticker, close_a, low, high, date  = line.split("\t")

    # Blocco di conversione dei dati a partire dal formato stringa
    try:
        close_a = float(close_a)
    except ValueError:
        continue

    try:
        low = float(low)
    except ValueError:
        continue

    try:
        high = float(high)
    except ValueError:
        continue

    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except Exception:
        continue

    # Managment della mappa
    if ticker not in action_map:
        action_map[ticker] = {'first_date':date, 'last_date':date, 'var':0,
            'max_price':high, 'min_price':low, 'first_close': close_a, 'last_close':close_a} 
    else:
        if action_map[ticker]['first_date']>date:
            action_map[ticker]['first_date']=date
            action_map[ticker]['first_close']=close_a
            action_map[ticker]['var']=percentage_change(close_a, action_map[ticker]['last_close'])
        
        if action_map[ticker]['last_date']<date:
            action_map[ticker]['last_date']=date
            action_map[ticker]['last_close']=close_a
            action_map[ticker]['var']=percentage_change(action_map[ticker]['first_close'], close_a)
        
        if action_map[ticker]['max_price']<high:
            action_map[ticker]['max_price']=high

        if action_map[ticker]['min_price']>low:
            action_map[ticker]['min_price']=low

# Ordinamento del risultato secondo l'ordine decrescende di ultima chiusura
sorted_list = sorted(action_map.items(), reverse=True ,key=lambda x: (x[1]['last_date']))

for item in sorted_list:
    out_str = f"Ticket: {item[0]}"
    for key in item[1]:
        out_str += f", {key}: {item[1][key]}"
    print(out_str)
