import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

import sys
sys.path.append("../")

from app import app


layout = html.Div(
            id='graph',
            className="five columns"
        )