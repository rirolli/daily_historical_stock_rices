#!/usr/bin/env python3
"""reducer.py"""

# per ciascun settore e per ciascun anno (dal 2009 al 2018):
# (a) la variazione percentuale(1) della quotazione del settore(2) nell'anno,
# (b) l'azione del settore che ha avuto il maggior incremento percentuale(1)
#     nell'anno (con indicazione dell'incremento),
# (c) l'azione del settore che ha avuto il maggior volume di transazioni
#     nell'anno (con indicazione del volume).
# 
# Il report deve essere ordinato per nome del settore.
# 
# (1) La variazione percentuale è la differenza percentuale tra il primo e
#     l’ultimo prezzo di chiusura presente nell’archivio.
# (2) La quotazione di un settore si ottiene sommando le quotazioni (prezzo
#     di chiusura) di tutte le azioni del settore.

# struttura del dizionario utilizzato:
# {ANNO: {SETTORE:{'first_date':DATE
#                  'last_date':DATE
#                  'first_date_count_close':FLOAT,
#                  'last_date_count_close':FLOAT,
#                  'var_count_close':FLOAT,
#                  TICKER:{'first_date':DATE
#                          'last_date':DATE
#                          'first_date_close':FLOAT,
#                          'last_date_close':FLOAT,
#                          'volume':INT
#                         }
#        }
# }
# 
# {SETTORE:{ANNO:{'max_var_count_close':FLOAT,
#                 'max_var_act':FLOAT,
#                 'max_var_act_name':STRING,
#                 'max_vol_act':FLOAT,
#                 'max_vol_act_name':STRING
#                }
#          }
# }


import sys

from datetime import datetime

# Funzioni di supporto
def percentage_change(i,f):
    r = 100*((f-i)/i)
    return r

# Blocco di esecuzione del reducer.py
years_map = {}  # dizionario contenente le informazioni dei settori e delle azioni per ogni anno
return_map = {} # dizionario contenente i risultati della query allocato dinamicamente

for line in sys.stdin:
    # Gestione dei dati in input
    line = line.strip()

    ticker, close_a, volume, date, sector  = line.split("\t")

    if sector == "N/A":
        continue

    # Blocco di conversione dei dati a partire dal formato stringa
    try:
        close_a = float(close_a)
    except ValueError:
        continue

    try:
        volume = int(volume)
    except ValueError:
        continue

    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except Exception:
        continue

    # Gestione dei dizionari
    curr_year = date.year

    if curr_year not in years_map:
        years_map[curr_year] = {}

    if sector not in years_map[curr_year]:  # se il settore non è già nel dizionario viene aggiunto
        years_map[curr_year][sector] = {'first_date':date, 'last_date':date, 'first_date_count_close':close_a, 'last_date_count_close':close_a, 'var_count_close':0}
    else:   # se il settore è già nel dizionario vengono aggiornati tutti i dati
        if years_map[curr_year][sector]['first_date'] == date:  # controllo della prima data in quell'anno per il settore
            years_map[curr_year][sector]['first_date_count_close'] += close_a
            years_map[curr_year][sector]['var_count_close'] = percentage_change(years_map[curr_year][sector]['first_date_count_close'], years_map[curr_year][sector]['last_date_count_close'])
        elif date < years_map[curr_year][sector]['first_date']: # controllo della prima data in quell'anno per il settore
            years_map[curr_year][sector]['first_date'] = date
            years_map[curr_year][sector]['first_date_count_close'] = close_a
            years_map[curr_year][sector]['var_count_close'] = percentage_change(close_a, years_map[curr_year][sector]['last_date_count_close'])

        if date == years_map[curr_year][sector]['last_date']:   # controllo dell'ultima data in quell'anno per il settore
            years_map[curr_year][sector]['last_date_count_close'] += close_a
            years_map[curr_year][sector]['var_count_close'] = percentage_change(years_map[curr_year][sector]['first_date_count_close'], years_map[curr_year][sector]['last_date_count_close'])
        elif date > years_map[curr_year][sector]['last_date']:  # controllo dell'ultima data in quell'anno per il settore
            years_map[curr_year][sector]['last_date'] = date
            years_map[curr_year][sector]['last_date_count_close'] = close_a
            years_map[curr_year][sector]['var_count_close'] = percentage_change(years_map[curr_year][sector]['first_date_count_close'], close_a)
    # accumulatore di azioni
    if ticker not in years_map[curr_year][sector]:   # se il ticker non è già presente nel dizionario
        years_map[curr_year][sector][ticker] = {}
        years_map[curr_year][sector][ticker]['first_date'] = date
        years_map[curr_year][sector][ticker]['last_date'] = date
        years_map[curr_year][sector][ticker]['first_date_close'] = close_a
        years_map[curr_year][sector][ticker]['last_date_close'] = close_a
        years_map[curr_year][sector][ticker]['var'] = 0
        years_map[curr_year][sector][ticker]['volume'] = volume
    else:   # se il ticker è già presente nel dizionario
        if date < years_map[curr_year][sector][ticker]['first_date']:   # controllo della prima data in quell'anno per l'azione
            years_map[curr_year][sector][ticker]['first_date'] = date
            years_map[curr_year][sector][ticker]['first_date_close'] = close_a
            years_map[curr_year][sector][ticker]['var'] = percentage_change(close_a, years_map[curr_year][sector][ticker]['last_date_close'])
        elif date > years_map[curr_year][sector][ticker]['last_date']:  # controllo dell'ultima data in quell'anno per l'azione
            years_map[curr_year][sector][ticker]['last_date'] = date
            years_map[curr_year][sector][ticker]['last_date_close'] = close_a
            years_map[curr_year][sector][ticker]['var'] = percentage_change(years_map[curr_year][sector][ticker]['first_date_close'], close_a)

        years_map[curr_year][sector][ticker]['volume'] += volume    # aggiornamento del volume

    # Eventuale aggiornamento dei valori massimi
    if sector not in return_map:
        return_map[sector] = {}
    if curr_year not in return_map[sector]:
        return_map[sector][curr_year] = {'max_var_count_close':0, 'max_var_act':0, 'max_var_act_name':'', 'max_vol_act':0, 'max_vol_act_name':''}

    if years_map[curr_year][sector]['var_count_close'] > return_map[sector][curr_year]['max_var_count_close']:   # controllo e in nuovo var max del settore nell'anno è cambiato
        return_map[sector][curr_year]['max_var_count_close'] = years_map[curr_year][sector]['var_count_close']
    if years_map[curr_year][sector][ticker]['volume'] > return_map[sector][curr_year]['max_vol_act']:    # controllo se il volume massimo dell'azione nel settore corrente è cambiato
        return_map[sector][curr_year]['max_vol_act'] = years_map[curr_year][sector][ticker]['volume']
        return_map[sector][curr_year]['max_vol_act_name'] = ticker
    if years_map[curr_year][sector][ticker]['var'] > return_map[sector][curr_year]['max_var_act']:
        return_map[sector][curr_year]['max_var_act'] = years_map[curr_year][sector][ticker]['var']
        return_map[sector][curr_year]['max_var_act_name'] = ticker

# Ordinamento del risultato secondo l'ordine decrescende di ultima chiusura
sorted_list = sorted(return_map.items(), key = lambda kv:(kv[0], kv[1]))

# Standard Output per la memorizzazione dei dati
for item in sorted_list:
    out_str = f"Settore: {item[0]}"

    for key in item[1]:
        out_str += "\n\tanno: {},\tvariazione quotazione del settore: {:.2f}%,\tazione con il maggior incremento percentuale: {} ({:.2f}%),\tazione con il maggior volume: {} ({})".format(key, item[1][key]['max_var_count_close'], item[1][key]['max_var_act_name'], item[1][key]['max_var_act'], item[1][key]['max_vol_act_name'], item[1][key]['max_vol_act'])
    print(out_str)
