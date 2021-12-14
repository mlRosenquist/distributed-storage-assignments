import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# END TO END
df = pd.read_csv('./results/results_erasure.csv')
df = df.sort_values(by=['write_time'])

df_erasure_coding_rs = df[df.storage_mode == 'erasure_coding_rs']
df_erasure_coding_rs_1 = df_erasure_coding_rs[df_erasure_coding_rs.max_erasures == 1]
df_erasure_coding_rs_2 = df_erasure_coding_rs[df_erasure_coding_rs.max_erasures == 2]

df_erasure_coding_rs_random_worker = df[df.storage_mode == 'erasure_coding_rs_random_worker']
df_erasure_coding_rs_random_worker_1 = df_erasure_coding_rs_random_worker[df_erasure_coding_rs_random_worker.max_erasures == 1]
df_erasure_coding_rs_random_worker_2 = df_erasure_coding_rs_random_worker[df_erasure_coding_rs_random_worker.max_erasures == 2]

# PLOT - df_erasure_coding_rs

fig = go.Figure()
fig.add_trace(go.Histogram(x=df_erasure_coding_rs_1[(df_erasure_coding_rs_1.file_size == '10MB')].iloc[5:].iloc[:-5].write_time))
fig.add_trace(go.Histogram(x=df_erasure_coding_rs_random_worker_1[(df_erasure_coding_rs_random_worker_1.file_size == '10MB')].iloc[5:].iloc[:-5].write_time))

# Overlay both histograms
fig.update_layout(barmode='overlay')
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
fig.show()

fig = make_subplots(rows=5, cols=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_1[(df_erasure_coding_rs_1.file_size == '10KB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=1, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_1[(df_erasure_coding_rs_1.file_size == '100KB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=2, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_1[(df_erasure_coding_rs_1.file_size == '1MB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=3, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_1[(df_erasure_coding_rs_1.file_size == '10MB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=4, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_1[(df_erasure_coding_rs_1.file_size == '100MB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=5, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_2[(df_erasure_coding_rs_2.file_size == '10KB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=1, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_2[(df_erasure_coding_rs_2.file_size == '100KB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=2, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_2[(df_erasure_coding_rs_2.file_size == '1MB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=3, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_2[(df_erasure_coding_rs_2.file_size == '10MB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=4, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_2[(df_erasure_coding_rs_2.file_size == '100MB')].iloc[5:].iloc[:-5].write_time, nbinsx=10),
                row=5, col=2)

fig.show()

# PLOT - df_erasure_coding_rs_random_worker
fig = make_subplots(rows=5, cols=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_1[(df_erasure_coding_rs_random_worker_1.file_size == '10KB')].iloc[5:].iloc[:-5].write_time),
                row=1, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_1[(df_erasure_coding_rs_random_worker_1.file_size == '100KB')].iloc[5:].iloc[:-5].write_time),
                row=2, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_1[(df_erasure_coding_rs_random_worker_1.file_size == '1MB')].iloc[5:].iloc[:-5].write_time),
                row=3, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_1[(df_erasure_coding_rs_random_worker_1.file_size == '10MB')].iloc[5:].iloc[:-5].write_time),
                row=4, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_1[(df_erasure_coding_rs_random_worker_1.file_size == '100MB')].iloc[5:].iloc[:-5].write_time),
                row=5, col=1)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_2[(df_erasure_coding_rs_random_worker_2.file_size == '10KB')].iloc[5:].iloc[:-5].write_time),
                row=1, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_2[(df_erasure_coding_rs_random_worker_2.file_size == '100KB')].iloc[5:].iloc[:-5].write_time),
                row=2, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_2[(df_erasure_coding_rs_random_worker_2.file_size == '1MB')].iloc[5:].iloc[:-5].write_time),
                row=3, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_2[(df_erasure_coding_rs_random_worker_2.file_size == '10MB')].iloc[5:].iloc[:-5].write_time),
                row=4, col=2)

fig.add_trace(
    go.Histogram(x=df_erasure_coding_rs_random_worker_2[(df_erasure_coding_rs_random_worker_2.file_size == '100MB')].iloc[5:].iloc[:-5].write_time),
                row=5, col=2)

fig.show()

df = pd.read_csv('../results.csv')


