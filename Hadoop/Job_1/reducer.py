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

def percentage_change(i,f):
    r = 100*((f-i)/i)
    return r

action_map = {}

for line in sys.stdin:

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

for act, info in action_map.items():
    out_str = f"Ticket: {act}"
    
    for key in info:
        out_str += f"\t{key}: {info[key]}"
    
    print(out_str)

#for act in action_map:
#    print(f'{act}\t{action_map[act]['first_date']}\t{action_map[act]['last_date']}\t{action_map[act]['var']}\t{action_map[act]['max_price']}\t{action_map[act]['min_price']}')
#    print(type(action_map[act]))
#    print(f'{action_map[act].get('var')}')

