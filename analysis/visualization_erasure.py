import pandas as pd
import plotly.express as px
from plotly.graph_objs.histogram import Marker
from plotly.graph_objs.histogram.marker import ColorBar, Pattern
from plotly.subplots import make_subplots
import plotly.graph_objects as go


# Time from receiving a file to successful generation of redundancy (replicas or coded fragments)
def add_trace(fig, df, column, file_size, storage_mode, max_erasures, row, col, legendgroup ,showlegend, color):
    data = df[(df.storage_mode == storage_mode) &
                          (df.max_erasures == max_erasures) &
                          (df.file_size == file_size)].iloc[5:].iloc[:-5]
    fig.add_trace(
        go.Histogram(x=data[column],
                     name=legendgroup,
                     legendgroup=legendgroup,
                     showlegend=showlegend,
                     marker=Marker(color=color, line=dict(color='black', width=1))),
        row=row, col=col)

    return fig

# END TO END
df = pd.read_csv('./results/results_erasure.csv')

#region write_time
df = df.sort_values(by=['write_time'])
fig = make_subplots(
    rows=5,
    cols=2,
    subplot_titles=("size=10KB, l=1", "size=10KB, l=2",
                    "size=100KB, l=1", "size=100KB, l=2",
                    "size=1MB, l=1", "size=1MB, l=2",
                    "size=10MB, l=1", "size=10MB, l=2",
                    "size=100MB, l=1", "size=100MB, l=2"),
    x_title='seconds',
    y_title='counts',
    )

row = 1
col = 1
for file_size in ['10KB', '100KB', '1MB', '10MB', '100MB']:
    showLegend = True
    if row > 1:
        showLegend = False
    add_trace(fig, df, 'write_time', file_size, 'erasure_coding_rs', 1, row, col, 'Lead node' ,showLegend, 'lightgreen')
    add_trace(fig, df, 'write_time', file_size, 'erasure_coding_rs_random_worker', 1, row, col, 'Random worker node' , showLegend, 'lightblue')
    row += 1

row = 1
col = 2
for file_size in ['10KB', '100KB', '1MB', '10MB', '100MB']:
    add_trace(fig, df, 'write_time', file_size, 'erasure_coding_rs', 2, row, col, 'Lead node', False, 'lightgreen')
    add_trace(fig, df, 'write_time', file_size, 'erasure_coding_rs_random_worker', 2, row, col, 'Random worker node', False, 'lightblue')
    row += 1


# Overlay both histograms
fig.update_layout(barmode='overlay', title_text='Histograms of complete write time to generate redundancy', title_x=0.5)
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
fig.show()
#endregion

#region read_time
df = df.sort_values(by=['read_time'])
fig = make_subplots(
    rows=5,
    cols=2,
    subplot_titles=("size=10KB, l=1", "size=10KB, l=2",
                    "size=100KB, l=1", "size=100KB, l=2",
                    "size=1MB, l=1", "size=1MB, l=2",
                    "size=10MB, l=1", "size=10MB, l=2",
                    "size=100MB, l=1", "size=100MB, l=2"),
    x_title='seconds',
    y_title='counts',
    )

row = 1
col = 1
for file_size in ['10KB', '100KB', '1MB', '10MB', '100MB']:
    showLegend = True
    if row > 1:
        showLegend = False
    add_trace(fig, df, 'read_time', file_size, 'erasure_coding_rs', 1, row, col, 'Lead node' ,showLegend, 'lightgreen')
    add_trace(fig, df, 'read_time', file_size, 'erasure_coding_rs_random_worker', 1, row, col, 'Random worker node' , showLegend, 'lightblue')
    row += 1

row = 1
col = 2
for file_size in ['10KB', '100KB', '1MB', '10MB', '100MB']:
    add_trace(fig, df, 'read_time', file_size, 'erasure_coding_rs', 2, row, col, 'Lead node', False, 'lightgreen')
    add_trace(fig, df, 'read_time', file_size, 'erasure_coding_rs_random_worker', 2, row, col, 'Random worker node', False, 'lightblue')
    row += 1


# Overlay both histograms
fig.update_layout(barmode='overlay', title_text='Histograms of complete read time to retrieve file', title_x=0.5)
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
fig.show()
#endregion

# Lead Node internal timings
df = pd.read_csv('../results.csv')
#region encoding

# filter for wanted rows
filtered_df_lead_node = df[(df.event == 'erasure_write') & (df.storage_mode == 'erasure_coding_rs')]
filtered_df_lead_node = filtered_df_lead_node.sort_values(by=['time'])

filtered_df_random_node = df[(df.event == 'erasure_write_worker_response') & (df.storage_mode == 'erasure_coding_rs_random_worker')]
filtered_df_random_node = filtered_df_random_node.sort_values(by=['time'])
fig = make_subplots(
    rows=5,
    cols=2,
    subplot_titles=("size=10KB, l=1", "size=10KB, l=2",
                    "size=100KB, l=1", "size=100KB, l=2",
                    "size=1MB, l=1", "size=1MB, l=2",
                    "size=10MB, l=1", "size=10MB, l=2",
                    "size=100MB, l=1", "size=100MB, l=2"),
    x_title='seconds',
    y_title='counts',
    )

row = 1
col = 1
for file_size in [10000, 100000, 1000000, 10000000, 100000000]:
    showLegend = True
    if row > 1:
        showLegend = False
    add_trace(fig, filtered_df_lead_node, 'time', file_size, 'erasure_coding_rs', 1, row, col, 'Lead node' ,showLegend, 'lightgreen')
    add_trace(fig, filtered_df_random_node, 'time', file_size, 'erasure_coding_rs_random_worker', 1, row, col, 'Random worker node' , showLegend, 'lightblue')
    row += 1

row = 1
col = 2
for file_size in [10000, 100000, 1000000, 10000000, 100000000]:
    add_trace(fig, filtered_df_lead_node, 'time', file_size, 'erasure_coding_rs', 2, row, col, 'Lead node', False, 'lightgreen')
    add_trace(fig, filtered_df_random_node, 'time', file_size, 'erasure_coding_rs_random_worker', 2, row, col, 'Random worker node', False, 'lightblue')
    row += 1


# Overlay both histograms
fig.update_layout(barmode='overlay', title_text='Histograms of encoding time', title_x=0.5)
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
fig.show()
#endregion

#region decoding
#TODO
#endregion

#region Lead node complete it task when writing

# filter for wanted rows
filtered_df = df[(df.event == 'erasure_write')]
filtered_df = filtered_df.sort_values(by=['time'])

fig = make_subplots(
    rows=5,
    cols=2,
    subplot_titles=("size=10KB, l=1", "size=10KB, l=2",
                    "size=100KB, l=1", "size=100KB, l=2",
                    "size=1MB, l=1", "size=1MB, l=2",
                    "size=10MB, l=1", "size=10MB, l=2",
                    "size=100MB, l=1", "size=100MB, l=2"),
    x_title='seconds',
    y_title='counts',
    )

row = 1
col = 1
for file_size in [10000, 100000, 1000000, 10000000, 100000000]:
    showLegend = True
    if row > 1:
        showLegend = False
    add_trace(fig, filtered_df, 'time', file_size, 'erasure_coding_rs', 1, row, col, 'Lead node' ,showLegend, 'lightgreen')
    add_trace(fig, filtered_df, 'time', file_size, 'erasure_coding_rs_random_worker', 1, row, col, 'Random worker node' , showLegend, 'lightblue')
    row += 1

row = 1
col = 2
for file_size in [10000, 100000, 1000000, 10000000, 100000000]:
    add_trace(fig, filtered_df, 'time', file_size, 'erasure_coding_rs', 2, row, col, 'Lead node', False, 'lightgreen')
    add_trace(fig, filtered_df, 'time', file_size, 'erasure_coding_rs_random_worker', 2, row, col, 'Random worker node', False, 'lightblue')
    row += 1


# Overlay both histograms
fig.update_layout(barmode='overlay', title_text="Histograms of when the lead node's work is done", title_x=0.5)
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
fig.show()
#endregion










