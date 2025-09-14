from sql import execute_sql_query
import json
from tqdm import tqdm
from decimal import Decimal

def normalize_value(v):
    if isinstance(v, str):
        v_lower = v.strip().lower() # ignore case
        if v_lower in {"true", "yes", "1"}: # different logic for boolean like values
            return normalize_value(1)
        elif v_lower in {"false", "no", "0"}:
            return normalize_value(0)
        return v_lower
    
    if isinstance(v, bool): # normalize booleans to numbers
        return normalize_value(int(v))
    
    if isinstance(v, (int, float, Decimal)): # normalize all numbers to floats with precision 5
        return round(float(v), 5)
    
    return v

def normalize(row):
    return tuple(normalize_value(v) for v in row)

def normalize_results(results):
    # ignore duplicate rows
    return set(normalize(row) for row in results)

def is_similar(exec_a, exec_p):
    res_a = normalize_results(exec_a["data"])
    res_p = normalize_results(exec_p["data"])
        
    if res_a == res_p:
        return True
    
    count = 0
    for row_p in res_p:
        # ignore extra columns as long as data is the same
        if any(set(row_a).issubset(set(row_p)) for row_a in res_a) or \
           any(set(row_p).issubset(set(row_a)) for row_a in res_a): 
            count += 1
        else:
            return False
    return count == len(res_a)

predicts_filename = "output.json"
actuals_filename = "mini_dev_postgresql.json"

correct = 0
total = 0
with open(predicts_filename, "r") as pf, open(actuals_filename, "r") as af:
    predicts = json.load(pf)
    actuals = json.load(af)
    for p, a in (pbar := tqdm(list(zip(predicts, actuals)))):
        exec_a = execute_sql_query(a["SQL"])
        exec_p = execute_sql_query(p["SQL"])

        if "data" in exec_p and "data" in exec_a and is_similar(exec_a, exec_p):
            correct += 1
        
        total += 1
        pbar.set_postfix_str(f"ACCURACY: {correct}/{total} = {round(correct/total, 3)}")
    
    print(f"FINAL ACCURACY: {correct}/{total} = {round(correct/total, 3)}")