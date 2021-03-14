import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px

import numpy as np
import os
import sys
sys.path.append("../")

from app import app
from Evaluator.metrics import Precision
from utils import load_dict

layout = html.Div(
            id='graph-precision',
            className="five columns"
        )

@app.callback(Output('graph-precision', "children"),
              [
                Input('folder-dropdown', 'value'),
                Input('N-slider', 'value'),
                Input('K-slider', 'value'),
                Input('runs', 'value')
               ])
def update_graph(folder, N, K, runs):
    # load Recs and Test sets of File
    base_path = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/RecSys/Results"

    # Get data
    results = load_dict(folder)

    # Compute Precision and Average runs in results
    evaluator = Precision()
    #evaluator.get_value(results['run_0']['test'][:K], results['run_0']['recs'], [K])

    # TODO Cambiare le metriche per rendere un solo valore
    y_data = [evaluator.get_value(results[f'run_{i}']['test'], results['run_0']['recs'][:N], [K]) for i in range(runs)]
    y_data = [y for x in y_data for y in x]
    print(y_data)
    # Create figure 1: Normal
    fig1 = px.line(x=np.arange(0,runs), y=y_data, render_mode="webgl")

    # Create figure 2:  Cumulative
    fig2 = px.line(x=np.arange(0,runs), y=np.cumsum(y_data), render_mode="webgl")


    # Return a row with 2 graphs: one normal, one with cumulative
    return dbc.Row([
        dbc.Col(
            html.Div([
                html.H5('Normal'),
                dcc.Graph(
                    id='precision-normal',
                    figure=fig1
                )
            ], style={'marginBottom': 50, 'marginTop': 25, 'marginLeft':15, 'marginRight':15}
            )
        ),

        dbc.Col(
            html.Div([
                html.H5('Cumulative'),
                dcc.Graph(
                    id='precision-cumulative',
                    figure=fig2
                )
            ], style={'marginBottom': 50, 'marginTop': 25, 'marginLeft':15, 'marginRight':15}
            )
        )
    ])



