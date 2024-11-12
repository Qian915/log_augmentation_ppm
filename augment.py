import datetime
import pandas as pd
from ast import literal_eval


# augmentation function for df constraint
def augment_df(df, constraint):    
    case_id, activity, time = 'case:concept:name', 'concept:name', 'time:timestamp'
    df[time] = pd.to_datetime(df[time])     # convert timestamp strings to datetime objects
    # constraint infomation
    pre = constraint.loc[0, 'predecessor']
    suc = constraint.loc[0, 'successor']

    df_augmented = []
    unique_cases = df[case_id].unique()
    for case in unique_cases:
        trace = df[df[case_id] == case].copy()
        trace = trace.sort_index().reset_index(drop=True)
        # start augmenting    
        if pre in trace[activity].unique():
            indexes = trace[trace[activity] == pre].index.tolist()
            for i in indexes:
                trace.loc[i+0.5] = trace.loc[i]
                trace.loc[i+0.5, activity] = suc
            trace = trace.sort_index().reset_index(drop=True)
            df_augmented.append(trace)
        # no augmentation for normal cases
        else:
            df_augmented.append(trace)
    df_augmented = pd.concat(df_augmented, ignore_index=True)
    df_augmented[case_id] = df_augmented[case_id].astype(str)    # convert case_id to str
    df_augmented = df_augmented.sort_values(by=[case_id, time])    # sort cases by case_id
    return df_augmented

# augmentation function for ef constraint with min temporal limit
def augment_ef_min(df, constraint):
    case_id, activity, time = 'case:concept:name', 'concept:name', 'time:timestamp'
    df[time] = pd.to_datetime(df[time])     # convert timestamp strings to datetime objects
    # constraint infomation
    pre = constraint.loc[0, 'predecessor']
    suc = constraint.loc[0, 'successor']
    gran = constraint.loc[0, 'granularity']
    time_value = int(constraint.loc[0, 'time_value'])
    temp_limit = update_granularity(time_value, gran)   

    df_augmented = []
    unique_cases = df[case_id].unique()
    for case in unique_cases:
        trace = df[df[case_id] == case].copy()
        trace = trace.sort_index().reset_index(drop=True)    
        # start augmenting
        if pre in trace[activity].unique():
            augmented_traces = []
            indexes = trace[trace[activity] == pre].index.tolist()
            next_gap = 0.1
            for _, i in enumerate(indexes):
                if _ == 0:
                    counter = 0
                    trace_idx = trace.index.tolist()
                    if i == trace_idx[-1]:    # pre is the last event -> add from the pre
                        augmented_traces += inject(trace, suc, i, len(trace_idx), next_gap, trace_idx)
                    else:
                        for j in trace_idx[trace_idx.index(i)+1:]:    # next available
                            temp_distance = check_temporal_dist(trace, i, j)
                            if temp_distance < temp_limit:
                                if j == trace_idx[-1]:    # the next event is the end -> add from the next event j
                                    augmented_traces += inject(trace, suc, j, len(trace_idx), next_gap, trace_idx)
                                else:
                                    counter += 1
                                    continue
                            else:
                                if counter == 0:
                                    _from = i
                                else:
                                    _from = trace_idx[trace_idx.index(j)-1]
                                augmented_traces += inject(trace, suc, _from, len(trace_idx), next_gap, trace_idx)    # add from the pre
                                break
                else:
                    counter = 0
                    next_gap *= 0.1
                    subcases = augmented_traces.copy()
                    augmented_traces = []
                    for subcase in subcases:
                        trace_idx = subcase.index.tolist()
                        if i == trace_idx[-1]:    # pre is the last event -> add from the end
                            augmented_traces += inject(subcase, suc, i, len(trace_idx), next_gap, trace_idx)
                        else:    # check the temporal distance between pre and the next event
                            for j in trace_idx[trace_idx.index(i)+1:]:    # next available
                                temp_distance = check_temporal_dist(subcase, i, j)
                                if temp_distance < temp_limit:
                                    if j == trace_idx[-1]:    # the next event is the end -> add from the next event j
                                        augmented_traces += inject(subcase, suc, j, len(trace_idx), next_gap, trace_idx)
                                    else:
                                        counter += 1
                                        continue
                                else:
                                    if counter == 0:
                                        _from = i
                                    else:
                                        _from = trace_idx[trace_idx.index(j)-1]
                                    augmented_traces += inject(subcase, suc, _from, len(trace_idx), next_gap, trace_idx)    # add from the pre
                                    break
            df_augmented += augmented_traces
        # no augmentation for normal cases    
        else:
            df_augmented.append(trace)

    df_augmented = pd.concat(df_augmented, ignore_index=True)
    df_augmented[case_id] = df_augmented[case_id].astype(str)    # convert case_id to str
    df_augmented[time] = pd.to_datetime(df_augmented[time])
    df_augmented = df_augmented.sort_values(by=[case_id, time])    # sort cases by case_id

    return df_augmented

# augmentation function for ef constraint with max temporal limit
def augment_ef_max(df, constraint):
    case_id, activity, time = 'case:concept:name', 'concept:name', 'time:timestamp'
    df[time] = pd.to_datetime(df[time])     # convert timestamp strings to datetime objects
    # constraint infomation
    pre = constraint.loc[0, 'predecessor']
    suc = constraint.loc[0, 'successor']
    gran = constraint.loc[0, 'granularity']
    time_value = int(constraint.loc[0, 'time_value'])
    temp_limit = update_granularity(time_value, gran)   

    df_augmented = []
    unique_cases = df[case_id].unique()
    for case in unique_cases:
        trace = df[df[case_id] == case].copy()
        trace = trace.sort_index().reset_index(drop=True)    
        # start augmenting
        if pre in trace[activity].unique():
            augmented_traces = []
            indexes = trace[trace[activity] == pre].index.tolist()
            next_gap = 0.1
            for _, i in enumerate(indexes):
                if _ == 0:
                    trace_idx = trace.index.tolist()
                    if i == trace_idx[-1]:    # pre is the last event -> add till the end
                        augmented_traces += inject(trace, suc, i, len(trace_idx), next_gap, trace_idx)
                    else:
                        for j in trace_idx[trace_idx.index(i)+1:]:    # next available
                            temp_distance = check_temporal_dist(trace, i, j)
                            if temp_distance <= temp_limit:
                                if j == trace_idx[-1]:    # the next event is the end -> add till the end
                                    augmented_traces += inject(trace, suc, i, len(trace_idx), next_gap, trace_idx)
                                else:
                                    continue
                            else:
                                augmented_traces += inject(trace, suc, i, j, next_gap, trace_idx)    # add till the next
                                break
                else:
                    next_gap *= 0.1
                    subcases = augmented_traces.copy()
                    augmented_traces = []
                    for subcase in subcases:
                        trace_idx = subcase.index.tolist()
                        if i == trace_idx[-1]:    # pre is the last event -> add till the end
                            augmented_traces += inject(subcase, suc, i, len(trace_idx), next_gap, trace_idx)
                        else:    # check the temporal distance between pre and the next event
                            for j in trace_idx[trace_idx.index(i)+1:]:    # next available
                                temp_distance = check_temporal_dist(subcase, i, j)
                                if temp_distance <= temp_limit:
                                    if j == trace_idx[-1]:    # the next event is the end -> add till the end if the next is not the suc;  else till next
                                        if subcase.at[j, activity] == suc:
                                            _to = j
                                        else:
                                            _to = len(trace_idx)
                                        augmented_traces += inject(subcase, suc, i, _to, next_gap, trace_idx)
                                    else:
                                        continue
                                else:
                                    augmented_traces += inject(subcase, suc, i, j, next_gap, trace_idx)    # add till the next
                                    break
            df_augmented += augmented_traces
        # no augmentation for normal cases    
        else:
            df_augmented.append(trace)

    df_augmented = pd.concat(df_augmented, ignore_index=True)
    df_augmented[case_id] = df_augmented[case_id].astype(str)    # convert case_id to str
    df_augmented = df_augmented.sort_values(by=[case_id, time])    # sort cases by case_id

    return df_augmented

# augmentation function for ef constraint with interval temporal limit
def augment_ef_interval(df, constraint):
    case_id, activity, time = 'case:concept:name', 'concept:name', 'time:timestamp'
    df[time] = pd.to_datetime(df[time])     # convert timestamp strings to datetime objects
    # constraint infomation
    pre = constraint.loc[0, 'predecessor']
    suc = constraint.loc[0, 'successor']
    gran = constraint.loc[0, 'granularity']
    time_value = constraint.loc[0, 'time_value']
    time_value = literal_eval(time_value)
    time_value_lower = time_value[0]    # lower bound
    time_value_upper = time_value[1]    # upper bound
    temp_limit_lower = update_granularity(time_value_lower, gran)
    temp_limit_upper = update_granularity(time_value_upper, gran)

    df_augmented = []
    unique_cases = df[case_id].unique()
    for case in unique_cases:
        trace = df[df[case_id] == case].copy()
        trace = trace.sort_index().reset_index(drop=True)    
        # start augmenting
        if pre in trace[activity].unique():
            augmented_traces = []
            indexes = trace[trace[activity] == pre].index.tolist()
            next_gap = 0.1
            for _, i in enumerate(indexes):
                if _ == 0:
                    counter = 0
                    trace_idx = trace.index.tolist()
                    if i == trace_idx[-1]:    # pre is the last event -> add from the pre
                        augmented_traces += inject(trace, suc, i, len(trace_idx), next_gap, trace_idx)
                    else:    # check the temporal distance between pre and the next event
                        for j in trace_idx[trace_idx.index(i)+1:]:    # next available    # trace_idx[trace_idx.index(i)+1:] index of the next element
                            temp_distance = check_temporal_dist(trace, i, j)
                            if temp_distance < temp_limit_lower:
                                if j == trace_idx[-1]:    # the next event is the end -> add from the next event j till the end
                                    augmented_traces += inject(trace, suc, j, len(trace_idx), next_gap, trace_idx)
                                else:
                                    counter += 1
                                    continue
                            else:
                                if counter == 0:
                                    _from = i
                                else:
                                    _from = trace_idx[trace_idx.index(j)-1]
                                for p in trace_idx[trace_idx.index(_from)+1:]:    # from is set, check the temporal dist between pre and the next
                                    temp_distance = check_temporal_dist(trace, i, p)
                                    if temp_distance <= temp_limit_upper:
                                        if p == trace_idx[-1]:    # the next event is the end -> add till the end
                                            augmented_traces += inject(trace, suc, _from, len(trace_idx), next_gap, trace_idx)
                                        else:
                                            continue
                                    else:
                                        augmented_traces += inject(trace, suc, _from, p, next_gap, trace_idx)    # add from the pre till the next
                                        break
                                break
                else:
                    counter = 0
                    next_gap *= 0.1
                    subcases = augmented_traces.copy()
                    augmented_traces = []
                    for subcase in subcases:
                        trace_idx = subcase.index.tolist()
                        if i == trace_idx[-1]:    # pre is the last event -> add from the pre till the end
                            augmented_traces += inject(subcase, suc, i, len(trace_idx), next_gap, trace_idx)
                        else:    # check the temporal distance between pre and the next event
                            for j in trace_idx[trace_idx.index(i)+1:]:    # next available    # trace_idx[trace_idx.index(i)+1:] index of the next element
                                temp_distance = check_temporal_dist(subcase, i, j)
                                if temp_distance < temp_limit_lower:
                                    if j == trace_idx[-1]:    # the next event is the end -> add from the next event j till the end
                                        augmented_traces += inject(subcase, suc, j, len(trace_idx), next_gap, trace_idx)
                                    else:
                                        counter += 1
                                        continue
                                else:
                                    if counter == 0:
                                        _from = i
                                    else:
                                        _from = trace_idx[trace_idx.index(j)-1]
                                    for p in trace_idx[trace_idx.index(_from)+1:]:    # from is set, check the temporal dist between pre and the next
                                        temp_distance = check_temporal_dist(subcase, i, p)
                                        if temp_distance <= temp_limit_upper:
                                            if p == trace_idx[-1]:    # the next event is the end -> add till the end if the next is not the suc;  else till next
                                                if subcase.at[p, activity] == suc:
                                                    _to = j
                                                else:
                                                    _to = len(trace_idx)
                                                augmented_traces += inject(subcase, suc, _from, _to, next_gap, trace_idx)
                                            else:
                                                continue
                                        else:
                                            augmented_traces += inject(subcase, suc, _from, p, next_gap, trace_idx)    # add from the pre till the next
                                            break
                                    break
            df_augmented += augmented_traces
        # no augmentation for normal cases    
        else:
            df_augmented.append(trace)

    df_augmented = pd.concat(df_augmented, ignore_index=True)
    df_augmented[case_id] = df_augmented[case_id].astype(str)    # convert case_id to str
    df_augmented[time] = pd.to_datetime(df_augmented[time])
    df_augmented = df_augmented.sort_values(by=[case_id, time])    # sort cases by case_id

    return df_augmented

# function for granularity updates: DURATION
def update_granularity(time_limit, granularity):
    if granularity == "DAY":
        time_diff = datetime.timedelta(days=time_limit)
    if granularity == "HOUR":
        time_diff = datetime.timedelta(hours=time_limit)
    return time_diff

# function for checking temporal distance between pre and the next activity
def check_temporal_dist(trace, i, j):
    time = 'time:timestamp'
    temp_distance = trace.at[j, time] - trace.at[i, time]
    return temp_distance

# function for injecting unseen event
def inject(trace, successor, _from, _to, next_gap, trace_idx, augmented_traces=None):
    augmented_traces = []
    case_id, activity, time = "case:concept:name", "concept:name","time:timestamp"
    
    _from = trace_idx.index(_from)
    if _to != len(trace_idx):
        _to = trace_idx.index(_to)
    for _, j in enumerate(trace_idx[_from: _to]):
        trace_tmp = trace.copy()
        new_case_id = "{}-{}".format(trace_tmp.at[0, case_id], _)  # Create new case_id
        trace_tmp[case_id] = trace_tmp[case_id].replace(trace_tmp.at[0, case_id], new_case_id)  # Update case_id
        
        # Insert new row with successor
        new_row = trace_tmp.loc[j].copy()
        new_row[activity] = successor    # add suc
        trace_tmp.loc[j + next_gap] = new_row
        trace_tmp = trace_tmp.sort_index()
        augmented_traces.append(trace_tmp)
    return augmented_traces

