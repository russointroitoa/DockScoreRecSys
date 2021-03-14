import os
import sys

import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table
import pandas
from dash.dependencies import Input, Output

sys.path.append("../")

from app import app
# from tabs import tab1, tab2

result_path = "/Users/alessiorussointroito/Documents/GitHub/DockScoreRecSys/RecSys/Results"

layout = html.Div([

    html.H1("Metrics"),

    dbc.Row([
        dbc.Col(
            html.Div([
                html.H5('Result Folder'),
                dcc.Dropdown(
                    id="folder-dropdown",
                    options=[{'label':x, 'value':x} for x in next(os.walk(result_path))[1]]
                    ),

                html.Br(),

                html.H5('Runs'),
                dcc.Dropdown(
                    id='runs',
                    options=[{'label':x, 'value':x} for x in range(10)]
                )
                ],
                style={'marginBottom': 50, 'marginTop': 25, 'marginLeft': 15, 'marginRight': 15}
            ),
            width=3
        ),

        dbc.Col(
            html.Div([
                html.H2("Parameters"),
                html.Div([
                    html.H5("N. Recommendations: N"),
                    dcc.Slider(
                        id="N-slider",
                        min=10,
                        max=50,
                        step=10,
                        marks={x: str(x) for x in range(10, 60, 10)},
                        value=10
                        ),
                    ]),


                html.Div([
                    html.H5("N. test elements: K"),
                    dcc.Slider(
                        id="K-slider",
                        min=10,
                        max=50,
                        step=10,
                        marks={x: str(x) for x in range(10, 60, 10)},
                        value=10
                    ),

                ])
            ]
            ,
            style={'marginBottom': 50, 'marginTop': 25, 'marginLeft':15, 'marginRight':15}
            )
        #, width=15
        )
    ]),

    html.Div([
        dcc.Tabs(
                id="tabs",
                value=None,
                children=[
                dcc.Tab(label='Precision', value='tab-precision'),
                dcc.Tab(label='MAP', value='tab-map'),
                dcc.Tab(label='ROC_AUC', value='tab-rocauc'),
                dcc.Tab(label='Recall', value='tab-recall'),

                 ]),
        html.Div(id="tabs-content")
    ])
])



