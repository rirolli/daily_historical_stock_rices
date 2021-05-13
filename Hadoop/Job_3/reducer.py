#!/usr/bin/env python3
"""reducer.py"""

# Un job in grado di generare le coppie di aziende che si somigliano
# (sulla base di una soglia scelta a piacere) in termini di variazione
# percentuale mensile nell’anno 2017 mostrando l’andamento mensile delle due aziende.
# 
# Esempio:
# Soglia=1%, coppie:
# 1:{Apple, Intel}: GEN: Apple +2%, Intel +2,5%, FEB: Apple +3%, Intel +2,7%, MAR: Apple +0,5%, Intel +1,2%, ...;
# 2:{Amazon, IBM}: GEN: Amazon +1%, IBM +0,5%, FEB: Amazon +0,7%, IBM +0,5%, MAR: Amazon +1,4%, IBM +0,7%, ..

# actions_map = {MESE: {ticker: {first_day,
#                                last_day,
#                                first_day_close,
#                                last_day_close,
#                                var
#                               }
#                      }
# }
# couple_map = {(azione1, azione2): [T,F,T,T...] }

import sys

from datetime import datetime

# Soglia di somiglianza
THRESHOLD = 1

# Funzioni di supporto
def percentage_change(i,f):
    r = 100*((f-i)/i)
    return r

def similar_percentage_change(a, b, threshold=THRESHOLD):
    return (-threshold) <= (a-b) <= threshold

def default_simil_map(ticker_list):
    simil_map = {}
    for tic in ticker_list:
        simil_map[tic] = []
        simil_map[tic].extend(ticker_list)
        simil_map[tic].remove(tic)
    return simil_map


# Blocco di esecuzione del reducer.py
actions_map = {}    # dizionario contenente le informazioni dei settori e delle azioni per ogni anno
ticker_name = {}    # dizionario che associa ad ogni ticker il nome dell'azienda
ticker_months = {}  # dizionario che tiene traccia dei mesi in cui si presenta un'azione nel 2017


for line in sys.stdin:
    # Gestione dei dati in input
    line = line.strip()

    ticker, name, close_a, date  = line.split("\t")

    # Blocco di conversione dei dati a partire dal formato stringa
    try:
        close_a = float(close_a)
    except ValueError:
        continue

    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except Exception:
        continue

    # ritorno del mese e del giorno corrente
    curr_month = date.month
    curr_day = date.day

    # teniamo traccia dei mesi in cui si presenta una determinata azione
    # una condizione necessaria per la somiglianza è che due azione devono
    # essere uguali per tutti e 12 i mesi
    if ticker not in ticker_months:
        ticker_months[ticker] = []
        [ticker_months[ticker].append(False) for _ in range(12)]

    ticker_months[ticker][curr_month-1] = True
    

    # gestione dei dizionari
    if curr_month not in actions_map:
        actions_map[curr_month] = {}
    if ticker not in ticker_name:
        ticker_name[ticker] = name

    if ticker not in actions_map[curr_month]:
        actions_map[curr_month][ticker] = {'name':name, 'first_day':curr_day, 'last_day':curr_day, 'first_day_close':close_a, 'last_day_close':close_a, 'var':0}
    else:
        if curr_day <= actions_map[curr_month][ticker]['first_day']:
            actions_map[curr_month][ticker]['first_day'] = curr_day
            actions_map[curr_month][ticker]['first_day_close'] = close_a
            actions_map[curr_month][ticker]['var'] = percentage_change(close_a, actions_map[curr_month][ticker]['last_day_close'])
        elif curr_day >= actions_map[curr_month][ticker]['last_day']:
            actions_map[curr_month][ticker]['last_day'] = curr_day 
            actions_map[curr_month][ticker]['last_day_close'] = close_a
            actions_map[curr_month][ticker]['var'] = percentage_change(actions_map[curr_month][ticker]['first_day_close'], close_a)

# pseudocodice di quello che sta per avvenire:
# creo un dizionario di similarità dove lo inizializzo aggiungendo tutti
# i ticker che son simili a tutti quanti. Ogni qual volta due risultano non
# essere simili allora viene rimosso dalla lista di similarità
ticker_list = list(ticker_months.keys())
simil_map = default_simil_map(ticker_list)

for months_actions in actions_map.items():
    for i_action in months_actions[1].items():
        for j_action in months_actions[1].items():
            if i_action[0]==j_action[0]:                    # se le azioni sono uguali
                continue
            elif j_action[0] not in simil_map[i_action[0]]: # se l'azione NON è già presente (= è stato già constatato che non sono simili)
                continue
            elif not similar_percentage_change(i_action[1]['var'],j_action[1]['var']):  # se non hanno una variazione simile
                simil_map[i_action[0]].remove(j_action[0])

# rimozione delle coppie ((Apple, Intel) = (Intel, Apple))
# e delle coppie vuote ((Apple, Intel) = [])
# e delle azioni che non sono presenti in tutti e 12 i mesi
simil_map_copy = simil_map.copy()   # genero una copia di simil_map che ha scopo di iterator per modificare simil_map
for i_action in simil_map_copy.items():
    for j_action in simil_map_copy.items():
        if i_action[0] in simil_map:
            if j_action[0] in simil_map and (i_action[0] in simil_map[j_action[0]]) and (j_action[0] in simil_map[i_action[0]]): # se l'azione è già presente nella sua controparte (si evitano ripetizioni: (Apple, Intel) = (Intel, Apple))
                simil_map[i_action[0]].remove(j_action[0])
            if not all(ticker_months[j_action[0]]): # l'azione j non è presente in tutti e 12 i mesi dell'anno 2017
                if (j_action[0] in simil_map[i_action[0]]):
                    simil_map[i_action[0]].remove(j_action[0])
                if j_action[0] in simil_map:
                    del simil_map[j_action[0]]
            if not simil_map[i_action[0]] or not all(ticker_months[i_action[0]]):  # se la lista dele somiglianze è vuota allora elimino la coppia o se l'azione i non è presente in tutti e 12 i mesi dell'anno 2017
                del simil_map[i_action[0]]


# stampa dei risultati
actions_map_keys = list(actions_map.keys())                     # mesi nel dizionario
actions_map_keys = actions_map_keys.sort(key=lambda x:int(x))   # mesi del dizionario ordinati in modo ascendente

print(f"Soglia={THRESHOLD}%, coppie:")
for simil_map_items in simil_map.items():
    for act in simil_map_items[1]:
        return_str = f"({ticker_name[simil_map_items[0]]},{ticker_name[act]}):"
        for m in actions_map_keys:
            # if not(simil_map_items[0] in actions_map[m] and act in actions_map[m]): # se manca anche solo 1 mese allora non possono essere simili le aziende
            #     return_str = ""
            #     break
            ticker_name[simil_map_items[0]]
            actions_map[m][simil_map_items[0]]['var']
            ticker_name[act]
            actions_map[m][act]['var']
            return_str = return_str + "\t{}: {} {}%, {} {}%".format(m, ticker_name[simil_map_items[0]], actions_map[m][simil_map_items[0]]['var'], ticker_name[act], actions_map[m][act]['var'])
        print(return_str)