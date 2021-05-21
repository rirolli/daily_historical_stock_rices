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

def is_grown(o, c):
    return c>=o

# Blocco di esecuzione del reducer.py
action_map = {}                 # dizionario contenente le informazioni di ogni azione
meta_growth_days = {}           # dizionario contenenti le informazioni utili per il calcolo del numero di giorni di crescita consecutivi
sorted_meta_growth_days = {}    # versione ordinata di meta_growth_days
growth_days = {}                # dizionario che tiene traccia del numero di giorni di crescita consecutivi

for line in sys.stdin:
    # Gestione e conversione dei dati in input

    line = line.strip()

    ticker, open_a, close_a, low, high, date  = line.split("\t")

    # Blocco di conversione dei dati a partire dal formato stringa
    try:
        open_a = float(open_a)
    except ValueError:
        continue

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

    # ritorno del mese e del giorno corrente
    curr_year = date.year

    # Managment del dizionario
    if ticker not in action_map:
        action_map[ticker] = {'first_date':date, 'last_date':date, 'var':0,
            'max_price':high, 'min_price':low, 'first_close': close_a, 'last_close':close_a, 'days_of_growth':0, 'year_of_growth':0} 
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

    if ticker not in meta_growth_days:
        meta_growth_days[ticker] = [(date, is_grown(open_a, close_a))]
    else:
        meta_growth_days[ticker].append((date, is_grown(open_a, close_a)))

# Calcolo dei giorni di crescita consecutivi
# Il primo passaggio da fare è ordinare i dati; questo passaggio è importante
# perché non si può presupporre che la lettura fornisca i dati in ordine di data.
for key, value in meta_growth_days.items():
    sorted_meta_growth_days[key] = sorted(value ,key=lambda x: (x[0]))
# Ora si procede con il conteggio dei giorni consecutivi di crescita
for key in sorted_meta_growth_days.keys():
    growth_days[key] = {'current':0, 'current_year':0, 'max':0, 'max_year':0}   # Inizializzazione del dizionario
for key, values in sorted_meta_growth_days.items():
    for value in values:
        curr_year = value[0].year
        if value[1] is True:
            if curr_year == growth_days[key]['max_year']:
                growth_days[key]['current']+=1
            else:
                growth_days[key]['current']=1
                growth_days[key]['current_year']=curr_year

            if growth_days[key]['current']>growth_days[key]['max']:
                growth_days[key]['max'] = growth_days[key]['current']
                growth_days[key]['max_year'] = growth_days[key]['current_year']
        else:
            growth_days[key]['current']=0
            growth_days[key]['current_year']=curr_year
# Aggiunta del dato di giorni di crescita consecutivi al dizionario
for key in growth_days.keys():
    action_map[key]['days_of_growth'] = growth_days[key]['max']
    action_map[key]['year_of_growth'] = growth_days[key]['max_year']

# Ordinamento del risultato secondo l'ordine decrescende di ultima chiusura
sorted_list = sorted(action_map.items(), reverse=True ,key=lambda x: (x[1]['last_date']))

# Standard Output per la memorizzazione dei dati
for item in sorted_list:
    out_str = f"Ticker: {item[0]}"
    for key in item[1]:
        out_str += f", {key}: {item[1][key]}"
    print(out_str)
