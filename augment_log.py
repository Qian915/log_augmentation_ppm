import pandas as pd
import argparse
from augment import *


parser = argparse.ArgumentParser(
    description="Log Augmentation")

parser.add_argument("--dataset", 
    type=str, 
    default="Sepsis_C2", 
    help="dataset name")

args = parser.parse_args()

### 1. load event log ###
dataset = args.dataset               
log_name = dataset.split('_')[0]
log_path = f'{dataset}/{log_name}.csv'
df = pd.read_csv(log_path)
case_id, activity, time = 'case:concept:name', 'concept:name', 'time:timestamp'

### 2. load constraint ###
constraint_name = dataset.split('_')[-1]
constraint_path = f'{dataset}/{constraint_name}.xlsx'
constraint = pd.read_excel(constraint_path)
r = constraint.loc[0, 'relation_activity']

### 3. augment log ###
# for df
if r == 'df':
    print(f'augmenting log with {r} constraint:')
    log_augmented = augment_df(df, constraint)

# for ef
else:
    r_t = constraint.loc[0, 'relation_time']
    print(f'augmenting log with {r} constraint: {r_t}')
    if r_t == 'min':
        log_augmented = augment_ef_min(df, constraint)
    if r_t == 'max':
        log_augmented = augment_ef_max(df, constraint)
    if r_t == 'interval':
        log_augmented = augment_ef_interval(df, constraint)

log_augmented_path = f'{dataset}/{log_name}_augmented.csv'
log_augmented.to_csv(log_augmented_path, index=False)
print(f'#traces: {df[case_id].nunique()}')
print(f'#augmented traces: {log_augmented[case_id].nunique()}')