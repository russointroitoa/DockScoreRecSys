import numpy as np
import os
import sys

from dash import Dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

sys.path.append("../")

from app import app
from Layouts import tab_precision
from Layouts import sidepanel

app.layout = sidepanel.layout

@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')]
              )
def render_content(tab):
    if tab == 'tab-precision':
        return tab_precision.layout
    elif tab == 'tab-map':
        return
    elif tab == 'tab-rocauc':
        return
    elif tab == 'tab-recall':
        return



if __name__=="__main__":

    app.run_server(debug=True)