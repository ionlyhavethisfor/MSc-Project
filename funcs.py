# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 20:26:56 2024.

@author: caspe
"""
import base64
import zlib
import pickle
import plotly.graph_objects as go
import numpy as np
import pandas as pd


def unzip_store(filterdata):
    """Unzips the filter_store data."""
    filterdata = base64.b64decode(filterdata)
    filterdata = pickle.loads(zlib.decompress(filterdata))
    filterdata = tuple(filterdata)
    if len(filterdata) == 1:
        filterdata = (filterdata[0], filterdata[0])
    if len(filterdata) == 0:
        filterdata = ()
    return filterdata


def normalise(lst, vmax=60, vmin=10):
    """Normalises range of wordcloud word scores to reasonable pixel sizes."""
    if lst == []:
        return lst
    lmax = max(lst, key=lambda x: x[1])[1]
    lmin = min(lst, key=lambda x: x[1])[1]
    vrange = vmax-vmin
    lrange = lmax-lmin or 1
    for entry in lst:
        entry[1] = int(((entry[1] - lmin) / lrange) * vrange + vmin)
    return lst


def get_batch(lst, batch_size, batch_number):
    """Use for calculating the batch size."""
    start_index = batch_size * (batch_number - 1)
    return lst[start_index:start_index + batch_size]


def remove_trailing_brackets(text):
    """Use to clean up some attribute labels on chart."""
    boo = False
    if '(generic)' in text:
        text = text.removesuffix('(generic)')
        boo = True
    text = text.removeprefix('(u)')
    if text.endswith(')') and '(' in text:
        text = text[:text.rfind('(')].strip()
        if boo:
            return text + ' (generic)'
        return text
    return text


# def generate_trace(filterdata, df, color, answerorkeyword, cluster_bool):
#     """Create trace for map."""
#     if filterdata or filterdata == ():
#         filtered_df = df[df['PIQPersonID'].isin(filterdata)]
#         interntrace_df = filtered_df.groupby([
#             answerorkeyword, 'Latitude', 'Longitude'
#         ]).size().reset_index(name='count').sort_values(by='count', ascending=False)
#     interntrace_df['norm_count'] = interntrace_df['count']/sum(interntrace_df["count"])*20000

#     trace = go.Scattermap(
#         lat=interntrace_df["Latitude"],
#         lon=interntrace_df["Longitude"],
#         hovertext=interntrace_df[answerorkeyword],
#         cluster={'enabled': cluster_bool, 'step': 100, 'maxzoom': 5, 'sizesrc': 'marker_size'},
#         line={'color': 'black', 'width': 1},
#         marker={
#             'sizemin': 1,
#             'sizemode': 'area',
#             'color': color,
#             'size': interntrace_df['norm_count'],
#         },
#         customdata = interntrace_df['count'],
#         hovertemplate="%{hovertext}<br>Count: %{customdata}<extra></extra>",
#     )
#     return trace

# def generate_trace(filterdata, df, color, answerorkeyword, cluster_bool):
#     """Create trace for map."""
#     if filterdata or filterdata == ():
#         filtered_df = df[df['PIQPersonID'].isin(filterdata)]
#         interntrace_df = filtered_df.groupby([
#             answerorkeyword, 'Latitude', 'Longitude'
#         ]).size().reset_index(name='count').sort_values(by='count', ascending=False)
#     trace = go.Scattermap(
#         lat=interntrace_df["Latitude"],
#         lon=interntrace_df["Longitude"],
#         hovertext=interntrace_df[answerorkeyword],
#         #cluster={'enabled': cluster_bool, 'step': 100, 'maxzoom': 5, 'sizesrc': 'marker_size'},
#         line={'color': 'black', 'width': 1},
#         marker={
#             'size': interntrace_df['count'],
#             'sizemin': 4,
#             'sizemode': 'area',
#             'color': color,
#         },
#         customdata = interntrace_df['count'],
#         hovertemplate="%{hovertext}<br>Count: %{customdata}<extra></extra>",
#     )
#     return trace

def generate_trace(filterdata, df, color, answerorkeyword, cluster_bool):
    """Create trace for map."""
    if filterdata or filterdata == ():
        filtered_df = df[df['PIQPersonID'].isin(filterdata)]
        interntrace_df = filtered_df.groupby([
            answerorkeyword, 'Latitude', 'Longitude'
        ]).size().reset_index(name='count').sort_values(by='count', ascending=False)
        
    bins = [0, 20, 100, 200, 300, 400, 500, 600, 1000, float('inf')]  # These define the cut-off points

    # Define corresponding labels (must be one less than the number of bins)
    labels = [20, 50, 100, 200, 300, 400, 500, 600, 1000,]  # The assigned values

    interntrace_df['norm_count'] = pd.cut(interntrace_df['count'], bins=bins, labels=labels, include_lowest=True).astype(int)

    trace = go.Scattermap(
        lat=interntrace_df["Latitude"],
        lon=interntrace_df["Longitude"],
        hovertext=interntrace_df[answerorkeyword],
        cluster={'enabled': cluster_bool, 'step': 100, 'maxzoom': 5, 'sizesrc': 'marker_size'},
        line={'color': 'black', 'width': 1},
        marker={
            'sizemin': 1,
            'sizemode': 'area',
            'color': color,
            'size': interntrace_df['norm_count'],
        },
        customdata = interntrace_df['count'],
        hovertemplate="%{hovertext}<br>Count: %{customdata}<extra></extra>",
    )
    return trace