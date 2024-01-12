import pandas as pd

metrics = {'metric_id': [1, 2, 3, 4, 5, 6],
           'metric': ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']}

metrics_df = pd.DataFrame(data=metrics)

print(metrics_df)

weeks = {'week_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           'week_number': ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']}

weeks_df = pd.DataFrame(data=weeks)

print(weeks_df)

for row in merged_df:
    if

subset_weeks = merged_df.iloc[:, 25:84]{}