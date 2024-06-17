import dash
from dash import html, dcc, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import mysql.connector
from datetime import datetime, date, timedelta
import datetime
import locale
from flask import Flask
from waitress import serve
from dash_bootstrap_templates import ThemeSwitchAIO
from dash.exceptions import PreventUpdate
import threading
import hashlib
import os
from dash import dash_table
from dotenv import load_dotenv
load_dotenv()


app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = 'Dashboard MODAbank'
tab_card = {'height': '100%'}

main_config = {
    "hovermode": "x unified",
    "legend": {"yanchor":"top",
                "y":0.9,
                "xanchor":"left",
                "x":0.1,
                "title": {"text": None},
                #"font" :{"color":"white"},
                "bgcolor": "rgba(0,0,0,0.5)"},
    "margin": {"l":10, "r":10, "t":30, "b":5}
}

config_graph={"displayModeBar": False, "showTips": False}

template_theme1 = "flatly"
template_theme2 = "darkly"
url_theme1      = dbc.themes.FLATLY
url_theme2      = dbc.themes.DARKLY
lock = threading.Lock()

host = os.environ['host']
user = os.environ['user']
senha = os.environ['senha']
database = os.environ['database']

def obter_dados_firebird():
    conexao = mysql.connector.connect(
        host=host,
        user=user,
        password=senha,
        database=database 
    )

    query = """
    SELECT DAY(c.data_pagamento) AS DIA,
        MONTH(c.data_pagamento) AS MES,
        YEAR(c.data_pagamento) AS ANO,
        e.Fantasia,
        c.status,
        'PIX_IN',
        c.valor,
        c.taxa_total,
        c.valor_sem_taxa AS VALOR_MENOS_TAXA,
        c.fk_empresa,
        ws.SALDO as saldo_atual
    FROM cobranca c
    INNER JOIN 
        empresa e ON c.fk_empresa = e.codigo
    LEFT JOIN 
        wl_saldo ws ON c.fk_empresa = ws.codigo

    UNION ALL

    SELECT DAY(s.data_solicitacao) AS DIA,
        MONTH(s.data_solicitacao) AS MES,
        YEAR(s.data_solicitacao) AS ANO,
        e.Fantasia,
        s.status,
        'PIX_OUT',
        s.valor_solicitado,
        s.taxa_total,
        s.valor_sem_taxa AS VALOR_MENOS_TAXA,
        s.fk_empresa,
        ws.SALDO as saldo_atual
    FROM saque s
    INNER JOIN 
         empresa e ON s.fk_empresa = e.codigo
    LEFT JOIN 
         wl_saldo ws ON s.fk_empresa = ws.codigo;
        """

    df = pd.read_sql(query, conexao)
    conexao.close()
    return df

def criacao():
        conexao = mysql.connector.connect(
            host=host,
            user=user,
            password=senha,
            database=database 
        )

        query2 = """
        
            SELECT DAY(c.data_dia) AS DIA_CRIACAO, 
                MONTH(c.data_dia) AS MES_CRIACAO, 
                YEAR(c.data_dia) AS ANO_CRIACAO
            FROM cobranca c
            INNER JOIN empresa e ON c.fk_empresa = e.codigo

            UNION ALL

            SELECT DAY(s.data_solicitacao) AS DIA_CRIACAO, 
                MONTH(s.data_solicitacao) AS MES_CRIACAO, 
                YEAR(s.data_solicitacao) AS ANO_CRIACAO
            FROM saque s
            INNER JOIN empresa e ON s.fk_empresa = e.codigo;

            """
        df2 = pd.read_sql(query2, conexao)
        
        conexao.close()
        return df2

df = obter_dados_firebird()
df_cru = df
df2 = criacao()

empresa_selecionada = None
def convert_to_text(month):
    match month:
        case 0:
            x = 'MÊS ATUAL'
        case 1:
            x = 'JAN'
        case 2:
            x = 'FEV'
        case 3:
            x = 'MAR'
        case 4:
            x = 'ABR'
        case 5:
            x = 'MAI'
        case 6:
            x = 'JUN'
        case 7:
            x = 'JUL'
        case 8:
            x = 'AGO'
        case 9:
            x = 'SET'
        case 10:
            x = 'OUT'
        case 11:
            x = 'NOV'
        case 12:
            x = 'DEZ'
    return x

def convert_to_tipo(pix_type):
    match pix_type:
        case 'PIX_IN':
            x = 'CASH-IN'
        case 'PIX_OUT':
            x = 'CASH-OUT'
    return x

mes_atual = datetime.datetime.now().month
ano_atual = datetime.datetime.now().year

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

def year_filter_criacao(year_criacao):
    if year_criacao == 0:
        mask = df2['ANO_CRIACAO'].isin([datetime.datetime.now().year])
    else:
        mask = df2['ANO_CRIACAO'].isin([year_criacao])
    return mask

def month_filter_criacao(month_criacao):
    if month_criacao == 0:
        mask = df2['MES_CRIACAO'].isin([datetime.datetime.now().month])
    else:
        mask = df2['MES_CRIACAO'].isin([month_criacao])
    return mask

def formatar_reais(valor):
    return locale.currency(valor, grouping=True)

def year_filter(year):
    if year == 0:
        mask = df['ANO'].isin([datetime.datetime.now().year])
    else:
       mask = df['ANO'].isin([year])
    return mask

def month_filter(month):
    if month == 0:
        mask = df['MES'].isin([datetime.datetime.now().month])
    else:
       mask = df['MES'].isin([month])
    return mask


def year_month_filter(year, month):
    if year == 0 and month == 0:
        mask = df['ANO'].isin([datetime.datetime.now().year]) & df['MES'].isin([datetime.datetime.now().month])
    elif year == 0:
        mask = df['MES'].isin([month])
    elif month == 0:
        mask = df['ANO'].isin([year])
    else:
        mask = (df['ANO'] == year) & (df['MES'] == month)
    return mask

def team_filter(team):
    if team == 0:
        mask = df['Fantasia'].isin(df['Fantasia'].unique())
    else:
        mask = df['Fantasia'].isin([team])
    return mask

def pix_filter(pix_type):
    if pix_type == 'PIX_IN':
        mask = df['PIX_IN'] == 'PIX_IN'
    elif pix_type == 'PIX_OUT':
        mask = df['PIX_IN'] == 'PIX_OUT'
    else:
        mask = df['PIX_IN'].isin(['PIX_IN', 'PIX_OUT'])
    return mask

def pix_filter_in(pix_type_in):
    if pix_type_in == 'PIX_IN':
        mask = df['PIX_IN'] == 'PIX_IN'
    else:
        mask = df['PIX_IN'].isin(['PIX_IN'])
    return mask

def status_pix_filter(status_list):
    if isinstance(status_list, str):
        status_list = [status_list]

    if 'Todos' in status_list:
        mask = df['status'].notnull()
    else:
        mask = df['status'].isin(status_list)
    return mask

start_date_default = date.today().replace(day=1)
end_date_default = date.today().replace(day=1).replace(month=start_date_default.month % 12 + 1) - timedelta(days=1)

center_style = {'display': 'flex', 'justify-content': 'center', 'align-items': 'center', 'height': '100vh'}

tab_graficos_fiscais = dbc.Tab(
    label="Extrato", tab_id="tab-graficos-fiscais", children=[
dbc.Container(fluid=True, children=[
    dbc.Row([
        dbc.Col([
                dbc.CardBody([
                    dbc.Row(
                        dbc.Col(
                            html.Legend('EXTRATO DIÁRIO')
                            )
                        ),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    dbc.Row(children=[
                                        dbc.Col([
                                            html.H5('Selecione o Período:'),
                                            dcc.DatePickerRange(
                                                id='date-range',
                                                start_date=start_date_default,
                                                end_date=end_date_default,
                                                display_format='DD/MM/YYYY'
                                            ),
                                        ], sm=6, lg=3), 
                                        dbc.Col([
                                            html.Div(
                                                id='table-container-in-parent',
                                                style={'display': 'flex', 'justify-content': 'center', 'margin-top': '7px'},
                                                children=[
                                                    html.Div(id='table-container-in'),    

                                                ]
                                            )
                                        ], sm=6, lg=6),
                                    ])
                                ])
                            ], style=tab_card)
                        ], sm=12, lg=12),
                ])
        ], sm=12, lg=12),
    ], className='g-2 my-auto', style={'margin-top': '7px'}),
        dcc.Interval(
        id='intervalo-component',
        interval=5 * 60 * 1000,
        n_intervals=0
    )
])], style={'height': '100vh'})


main_layout = dbc.Container(fluid=True, children=[
        dcc.Store(id='loading-state', data=True),  # Store to keep track of initial loading state
    dcc.Loading(
        id='loading-indicator',
        type='default',
        children=html.Div(id="app-content", children=[
    dbc.Tabs(id="tabs", active_tab="tab-graficos-vendas", children=[
        dbc.Tab(label="Gráficos Vendas", tab_id="tab-graficos-vendas"),
        tab_graficos_fiscais,
    ]),
    html.Div(id="graphs-container")
        ])
    )
])


login_layout = html.Div(
    [
        html.Div(
            dbc.Card(
                dbc.CardBody(
                    [html.Div(children=[
                        html.Img(src=r'assets/logo.png', alt='logo', className='logo input1'),
                        html.Br(),
                        dbc.Input(id='username-input', type='text', placeholder='Usuário', className='input'),
                        html.Br(),
                        dbc.Input(id='password-input', type='password', placeholder='Senha', className='input'),
                        html.Br(),
                        dbc.Button("Entrar", id='login-button', style={'background-color': '#e61a55', 'border': 'none'}, className='logo input'),
                        html.Div(id='login-output')
                    ])]
                )
            ), style={'max-width': '400px'}
        ),
    ], style=center_style
)


tab_graficos_vendas = dbc.Tab(
    label="Gráficos Vendas", id="tab-graficos-vendas", children=[
    dbc.Container(fluid=True, children=[
    dbc.Row([   html.Link(
        rel='shortcut icon',
        href='/assets/favicon.ico'
    ), 
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Legend("MODAbank")
                        ], sm=8),
                        dbc.Col([
                            html.I(className='logo', style={'font-size': '300%'})
                        ], sm=4, align="center")
                    ]),
                    dbc.Row([
                        dbc.Col([
                            ThemeSwitchAIO(aio_id="theme", themes=[url_theme1, url_theme2]),
                            html.Legend("DashBoard de Vendas")
                        ])
                    ], style={'margin-top': '10px'}),
                    dbc.Row([
                        html.Div(
                            className='logo-container',
                            children=[
                                html.Img(src='/assets/logomoda.png', alt='logo', className='logo'),
                            ]),
                        html.Div(
                            className='button-container',
                            children=[
                                dbc.Button("Sair", id='logout-button', style={'background-color': '#e61a55', 'border': 'none', 'margin-top':'15px'}, className='logo input'),
                                html.Div(id='logout-output')
                            ]),
                    ], style={'margin-top': '10px'})
                ])
            ], style=tab_card)
        ], sm=4, lg=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row(
                        dbc.Col(
                            html.Legend('FATURAMENTO POR MÊS')
                        )
                    ),
                    dbc.Row([
                        dbc.Col([
                            dcc.Graph(id='graph1', className='dbc', config=config_graph)
                        ], sm=12, md=7),
                        dbc.Col([
                            dcc.Graph(id='graph2', className='dbc', config=config_graph)
                        ], sm=12, lg=5)
                    ])
                ])
            ], style=tab_card)
        ], sm=12, lg=7),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row(
                        dbc.Col([
                            html.H5('Escolha o ANO'),
                            dbc.RadioItems(
                                id="radio-year",
                                options=[],
                                value=ano_atual if ano_atual in df['ANO'].unique() else 0,
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='year-selecty', style={'text-align': 'center', 'margin-top': '30px'}, className='dbc'),
                            html.H5('Escolha o MÊS'),
                            dbc.RadioItems(
                                id="radio-month",
                                options=[],
                                value=mes_atual if mes_atual in df['MES'].unique() else 0,
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='month-select', style={'text-align': 'center', 'margin-top': '30px'}, className='dbc'),
                            html.H5('Escolha o tipo de transação PIX'),
                                dbc.RadioItems(
                                id="radio-pix",
                                options=[],
                                value='Ambos',
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='radio-status-pix')
                        ])
                    )
                ])
            ], style=tab_card)
        ], sm=12, lg=3)
    ], className='g-2 my-auto', style={'margin-top': '7px'}),

    dbc.Row([
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            dcc.Graph(id='graph3', className='dbc', config=config_graph)
                        ])
                    ], style=tab_card)
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            dcc.Graph(id='graph4', className='dbc', config=config_graph)
                        ])
                    ], style=tab_card)
                ])
            ], className='g-2 my-auto', style={'margin-top': '7px'})
        ], sm=12, lg=5),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                            dcc.Graph(id='graph5', className='dbc', config=config_graph)
                    ], style=tab_card)
                ], sm=12),
                dbc.Col([
                    dbc.Card([
                            dcc.Graph(id='graph6', className='dbc', config=config_graph)
                    ], style=tab_card)
                ], sm=12)
            ], className='g-2'),
        ], sm=12, lg=4),
        dbc.Col([
            dbc.Card([
                dcc.Graph(id='graph8', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3)
    ], className='g-2 my-auto', style={'margin-top': '7px'}),

    dbc.Row([
        dbc.Col([
            dbc.Card([

                    dcc.Graph(id='graph10', className='dbc', config=config_graph)

            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph13', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph11', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([

                    dcc.Graph(id='graph12', className='dbc', config=config_graph)
                    ,html.Div(id="output-dados"),
            ], style=tab_card)
        ], sm=12, lg=3)
    ], className='g-2 my-auto', style={'margin-top': '7px'}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph14', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=4),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph16', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=4),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph17', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=4),
    ], className='g-2 my-auto', style={'margin-top': '7px'}),
        dcc.Interval(
        id='interval-component',
        interval=1 * 60 * 60 * 1000,
        n_intervals=0
    )
])], style={'height': '100vh'})



@app.callback(
    Output('graphs-container', 'children'),
    [Input('tabs', 'active_tab')]
)
def update_graphs_content(active_tab):
    if active_tab == 'tab-graficos-vendas':
        return tab_graficos_vendas.children
    elif active_tab == 'tab-graficos-fiscais':
        return None
    
            # SELECT codiemp AS codigo FROM usuarios
            # WHERE email = %s AND senha = %s

def authenticate_user(username, password):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=senha,
            database=database 
        )

        cursor = conn.cursor()
        sql = """
                SELECT e.codigo
                FROM empresa e
                INNER JOIN system_unit uni ON e.system_unit_id = uni.id
                INNER JOIN system_user su ON uni.id = su.system_unit_id
                WHERE su.login = %s AND su.password = %s

        """
        cursor.execute(sql, (username.strip(), hashlib.md5(password.encode()).hexdigest().upper()))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return True, result[0]
        else:
            return False, None

    except Exception as e:
        print(f"Erro ao autenticar usuário: {e}")
        return False, None



@app.callback(
    [Output('login-output', 'children'),
     Output('authenticated-store', 'data')],
    [Input('login-button', 'n_clicks')],
    [State('username-input', 'value'),
     State('password-input', 'value')],
    prevent_initial_call=True
)
def check_login(n_clicks, username, password):
    if n_clicks and username and password:
        authenticated, empresa_selecionada = authenticate_user(username, password)
        if authenticated:
            return dcc.Location(pathname=f'/main_layout/{empresa_selecionada}', id='main_layout_redirect'), empresa_selecionada
        else:
            return 'Credenciais inválidas', False
    return '', False



@app.callback(
    Output('extrato-dataframe', 'data'),
    [Input('authenticated-store', 'data')]
)
def load_extrato_data(empresa_selecionada):
    df_extrato_in = cosultaextratoin(empresa_selecionada)
    return df_extrato_in.to_dict('records')


def cosultaextratoin(empresa_selecionada):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=senha,
        database=database 
    )

    sql = """
            WITH 
                cobranca_cte AS (
                    SELECT 
                        c.fk_empresa,
                        date(c.data_pagamento) as data_dia,
                        sum(c.valor) AS valor_in,
                        COUNT(*) AS qtd_in,
                        SUM(c.taxa_total) AS taxa_in,
                        SUM(c.valor_sem_taxa) AS menos_taxa_in,
                        sum(c.valor) / COUNT(*) AS ticket_medio_in
                    FROM 
                        cobranca c
                    WHERE 
                        c.status IN ('CONCLUIDO', 'CONCLUIDA') AND c.fk_empresa = %s
                    GROUP BY 
                        1, 2
                ),
                saque_cte AS (
                    SELECT 
                        s.fk_empresa, 
                        date(s.data_solicitacao) AS data_dia, 
                        sum(s.valor_solicitado) AS valor_out, 
                        COUNT(*) AS qtd_out, 
                        sum(s.taxa_total) AS taxa_out, 
                        sum(s.valor_sem_taxa) AS menos_taxa_out ,
                        sum(s.valor_solicitado) / COUNT(*) AS ticket_medio_out
                    FROM 
                        saque s
                    WHERE 
                        s.status IN ('executed', 'processing') AND s.fk_empresa = %s
                    GROUP BY 
                        1, 2
                ),
                combined_cte AS (
                    SELECT 
                        coalesce(c.fk_empresa, s.fk_empresa) as fk_empresa,
                        coalesce(c.data_dia, s.data_dia) as data_dia,
                        coalesce(valor_in, 0) as valor_in,
                        coalesce(qtd_in, 0) as qtd_in,
                        coalesce(taxa_in, 0) as taxa_in,
                        coalesce(menos_taxa_in, 0) as menos_taxa_in,
                        coalesce(valor_out, 0) as valor_out,
                        coalesce(qtd_out, 0) as qtd_out,
                        coalesce(taxa_out, 0) as taxa_out,
                        coalesce(menos_taxa_out, 0) as menos_taxa_out,
                        coalesce(ticket_medio_in, 0) as ticket_medio_in,
                        coalesce(ticket_medio_out, 0) as ticket_medio_out
                    FROM 
                        cobranca_cte c
                    LEFT JOIN 
                        saque_cte s
                    ON 
                        c.data_dia = s.data_dia
                    UNION
                    SELECT 
                        coalesce(c.fk_empresa, s.fk_empresa) as fk_empresa,
                        coalesce(c.data_dia, s.data_dia) as data_dia,
                        coalesce(valor_in, 0) as valor_in,
                        coalesce(qtd_in, 0) as qtd_in,
                        coalesce(taxa_in, 0) as taxa_in,
                        coalesce(menos_taxa_in, 0) as menos_taxa_in,
                        coalesce(valor_out, 0) as valor_out,
                        coalesce(qtd_out, 0) as qtd_out,
                        coalesce(taxa_out, 0) as taxa_out,
                        coalesce(menos_taxa_out, 0) as menos_taxa_out,
                        coalesce(ticket_medio_in, 0) as ticket_medio_in,
                        coalesce(ticket_medio_out, 0) as ticket_medio_out
                    FROM 
                        cobranca_cte c
                    RIGHT JOIN 
                        saque_cte s
                    ON 
                        c.data_dia = s.data_dia
                ),
                saldo_cte AS (
                    SELECT 
                        fk_empresa,
                        data_dia,
                        valor_in,
                        qtd_in,
                        taxa_in,
                        menos_taxa_in,
                        valor_out,
                        qtd_out,
                        taxa_out,
                        menos_taxa_out,
                        ticket_medio_in,
                        ticket_medio_out,
                        SUM(menos_taxa_in - menos_taxa_out) OVER (PARTITION BY fk_empresa ORDER BY data_dia) as saldo_acumulado
                    FROM 
                        combined_cte
                )
            SELECT 
                *
            FROM 
                saldo_cte
            ORDER BY 
                data_dia asc;
    """

    df_extrato_in = pd.read_sql(sql, conn, params=(empresa_selecionada, empresa_selecionada,))

    conn.close()

    return df_extrato_in



@app.callback(
    Output("output-dados", "children"),
    Input('interval-component', 'n_intervals'),
    [Input('authenticated-store', 'data')] 
)
def recarregar_dados(n_intervals, empresa_selecionada):
    global df, df_extrato_in
    with lock:
        try:
            df = obter_dados_firebird()
            df2 = criacao()
            df_extrato_in = cosultaextratoin(empresa_selecionada)
        except Exception as e:
            print(f"Erro ao obter dados do Firebird: {e}")
    return None

df = obter_dados_firebird()

@app.callback(
    Output("radio-pix", "options"),
    Output("radio-pix", "value"),
    Input('interval-component', 'n_intervals')
)
def update_radio_pix(n_intervals):
    options = [{'label': 'CASH-IN', 'value': 'PIX_IN'}, {'label': 'CASH-OUT', 'value': 'PIX_OUT'}, {'label': 'CASH-IN E CASH-OUT', 'value': 'Ambos'}]
    default_value = 'Ambos'
    return options, default_value

@app.callback(
    Output("radio-status-pix", "options"),
    Output("radio-status-pix", "value"),
    Input('interval-component', 'n_intervals'),
)
def update_radio_status_pix(n_intervals):
    unique_status = df['status'].unique()
    options = [{'label': status, 'value': status} for status in unique_status]
    options.append({'label': 'TODOS', 'value': 'Todos'})
    default_value = ['Todos']
    return options , default_value

@app.callback(
    [Output("radio-year", "options"),
     Output("radio-year", "value")],
    [Input('interval-component', 'n_intervals')]
)
def update_year_options(n_intervals):
    with lock:
        try:
            df_filtered = df.dropna(subset=['ANO']).loc[df['ANO'] != '']

            unique_years = sorted(df_filtered['ANO'].unique(), reverse=True)
            options_year = [{'label': i, 'value': i} for i in unique_years]
            return options_year, datetime.datetime.now().year
        except Exception as e:
            print(f"Erro ao obter dados do ano: {e}")
            return [], None

@app.callback(
    [Output("radio-month", "options"),
     Output("radio-month", "value")],
    [Input('radio-year', 'value'),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]

)
def update_month_options(selected_year, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data 
    with lock:
        try:
            mes_atual = datetime.datetime.now().month
            ano_atual = datetime.datetime.now().year
            selected_year = selected_year or ano_atual

            if selected_year:
                df_filtered = df[(df['ANO'] == selected_year) & (df['fk_empresa'] == empresa_selecionada)]

                unique_months = sorted(df_filtered['MES'].unique())


                options_month = [{'label': convert_to_text(i), 'value': i} for i in unique_months]

                if options_month:
                    if selected_year == ano_atual:
                        default_month = mes_atual
                    else:
                        default_month = options_month[0]['value']
                    return options_month, default_month
                else:
                    return [], None
            else:
                return [], None
        except Exception as e:
            print(f"Erro ao obter dados do mês: {e}")
            return [], None


@app.callback(
    [Output('graph1', 'figure'),
     Output('graph2', 'figure')],
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graphs1e2(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_meseano2 = df.loc[(mask_year & mask_pix & mask_status_pix & mask_status) & (df['fk_empresa'] == empresa_selecionada)]

            df_1 = df_meseano2.groupby('MES')['valor'].sum().reset_index()
            df_1['MES'] = df_1['MES'].apply(convert_to_text)
            df_1['TOTAL_VENDAS'] = df_1['valor'].map(formatar_reais)
            fig1 = go.Figure(go.Bar(x=df_1['MES'], y=df_1['valor'], textposition='auto', text=df_1['TOTAL_VENDAS']))
            fig1.update_layout(main_config, height=350, template=template)
            
            df_2 = df_meseano2.groupby('MES')['valor'].sum().reset_index()
            df_2['MES'] = df_2['MES'].apply(convert_to_text)
            df_2['TOTAL_VENDAS'] = df_2['valor'].map(formatar_reais)
            fig2 = go.Figure(go.Pie(labels=df_2['MES'], values=df_2['valor'], text=df_2['TOTAL_VENDAS'], hoverinfo='label+percent'))
            fig2.update_traces(textposition='inside', textinfo='percent+label')
            fig2.update_layout(title='PORCENTAGEM POR MÊS', title_x=0.5, template=template, height=350)
        except Exception as e:
            print(f"Erro ao atualizar gráficos 1 e 2: {e}")
    return fig1, fig2


@app.callback(
     Output('graph3', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph3(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered2 = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix & mask_status) & (df['fk_empresa'] == empresa_selecionada)]

            df_3 = df_filtered2.groupby('DIA')['valor'].sum().reset_index()
            df_3['TOTAL_VENDAS'] = df_3['valor'].map(formatar_reais)
            fig3 = go.Figure(go.Scatter(x=df_3['DIA'], y=df_3['valor'], fill='tonexty', text=df_3['TOTAL_VENDAS'], hoverinfo='text'))
            fig3.add_annotation(text='FATURAMENTO POR DIA',xref="paper", yref="paper", font=dict( size=17, color='gray'), align="center", bgcolor="rgba(0,0,0,0.8)", x=0.05, y=0.85, showarrow=False)
            fig3.update_layout(main_config, height=180, template=template)

        except Exception as e:
            print(f"Erro ao atualizar gráficos 3: {e}")
    return fig3



@app.callback(
     Output('graph4', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph4(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_meseano2 = df.loc[(mask_year & mask_pix & mask_status_pix & mask_status) & (df['fk_empresa'] == empresa_selecionada)]

            df_4 = df_meseano2.groupby('MES')['valor'].sum().reset_index()
            df_4['TOTAL_VENDAS'] = df_4['valor'].map(formatar_reais)
            fig4 = go.Figure(go.Scatter(x=df_4['MES'], y=df_4['valor'], fill='tonexty', text=df_4['TOTAL_VENDAS'], hoverinfo='text'))
            fig4.add_annotation(text='MOVIMENTAÇÕES POR MÊS', xref="paper", yref="paper",font=dict( size=17, color='gray'),align="center", bgcolor="rgba(0,0,0,0.8)",x=0.05, y=0.85, showarrow=False)
            fig4.update_layout(main_config, height=180, template=template)

        except Exception as e:
            print(f"Erro ao atualizar gráficos 4: {e}")
    return fig4


@app.callback(
     Output('graph5', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph5(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered2 = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix & mask_status) & (df['fk_empresa'] == empresa_selecionada)]
            df_5 = df_filtered2.groupby(['Fantasia'])['valor'].mean() 
            df_5.sort_values(ascending=False, inplace=True)
            df_5 = df_5.reset_index()
            if not df_5.empty:
                total = df_5['valor'].iloc[0]
                total_ticket = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            else:
                total_ticket = 0.0
                
            fig5 = go.Figure()
            if toggle:
                fig5.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#306fc1'>TICKET MÉDIO</span><br><span style='font-size:70%; color:#306fc1'>Ticket médio mensal</span><br><br><span style='font-size:150%; color:#306fc1'>R${total_ticket}</span>"},
                        number_font={'color': 'white'},
                        value=0
                ))
            else:
                fig5.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#306fc1'>TICKET MÉDIO</span><br><span style='font-size:70%; color:#306fc1'>Ticket médio mensal</span><br><br><span style='font-size:150%; color:#306fc1'>R${total_ticket}</span>"},
                        number_font={'color': '#303030'},
                        value=0
                ))
            fig5.update_layout(main_config, height=210, template=template)
            fig5.update_layout({"margin": {"l": 0, "r": 0, "t": 150, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 5: {e}")
    return fig5

@app.callback(
     Output('graph6', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph6(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            df_filter = df[df['status'].isin(['CONCLUIDO', 'CONCLUIDA', 'executed', 'processing'])]
            df_6 = df_filter[df_filter['fk_empresa'] == empresa_selecionada].drop_duplicates(subset=['Fantasia'])
            
            df_6 = df_6[['Fantasia', 'saldo_atual']].rename(columns={'saldo_atual': 'saldo'})
            total = df_6['saldo'].sum()
            total_saldo = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig6 = go.Figure()
            if toggle:
                fig6.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#1cb49c'>SALDO TOTAL</span><br><span style='font-size:70%; color:#1cb49c'>Em Reais</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_saldo}</span>"},
                        number_font={'color': 'white'},
                        value=0
                ))
            else:
                fig6.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#1cb49c'>SALDO TOTAL</span><br><span style='font-size:70%; color:#1cb49c'>Em Reais</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_saldo}</span>"},
                        number_font={'color': '#303030'},
                        value=0
                ))
            fig6.update_layout(main_config, height=210, template=template)
            fig6.update_layout({"margin": {"l": 0, "r": 0, "t": 150, "b": 0}})


        except Exception as e:
            print(f"Erro ao atualizar gráficos 6: {e}")
    return fig6


@app.callback(
     Output('graph8', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph8(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter(year)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_meseano2 = df.loc[(mask_year & mask_pix & mask_status_pix & mask_status) & (df['fk_empresa'] == empresa_selecionada)]

            df_8 = df_meseano2.groupby('PIX_IN')['valor'].sum().reset_index()
            df_8['TOTAL_VENDAS'] = df_8['valor'].map(formatar_reais)
            df_8['PIX_IN'] = df_8['PIX_IN'].apply(convert_to_tipo)
            fig8 = go.Figure(go.Bar( x=df_8['valor'], y=df_8['PIX_IN'], orientation='h', textposition='auto', text=df_8['TOTAL_VENDAS'], hoverinfo='text',insidetextfont=dict(family='Times', size=12)))
            fig8.update_layout(main_config, height=400, template=template, title='CASH-IN E CASH-OUT POR ANO', margin=dict(t=50, b=0, l=0, r=0))
        except Exception as e:
            print(f"Erro ao atualizar gráficos 8: {e}")
    return fig8


@app.callback(
     Output('graph10', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph10(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)

            df_filtered = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix) & (df['fk_empresa'] == empresa_selecionada)]

            df_10 = df_filtered[(df_filtered['status'] == 'CONCLUIDO') | (df_filtered['status'] == 'CONCLUIDA') | (df_filtered['status'] == 'processing') | (df_filtered['status'] == 'executed')]
            transacoes_recebidas = df_10.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_recebidas).replace('.', ',').replace(',', '.', 1)
            total = df_10['valor'].sum()
            total_recebido = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig10 = go.Figure()
            if toggle:    
                fig10.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TRANSAÇÕES RECEBIDAS</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%;'>R${total_recebido}</span>"},
                                number_font={'color': 'white'}, 
                                value=0
                ))
            else:
                fig10.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TRANSAÇÕES RECEBIDAS</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%;'>R${total_recebido}</span>"},
                                number_font={'color': '#303030'}, 
                                value=0
                ))
            fig10.update_layout(main_config, height=170, template=template)
            fig10.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})


        except Exception as e:
            print(f"Erro ao atualizar gráficos 10: {e}")
    return fig10

@app.callback(
     Output('graph11', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph11(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)

            df_filtered = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix) & (df['fk_empresa'] == empresa_selecionada)]

            df_11 = df_filtered[(df_filtered['status'] == 'CONCLUIDO') | (df_filtered['status'] == 'CONCLUIDA') | (df_filtered['status'] == 'processing') | (df_filtered['status'] == 'executed')]
            transacoes_taxa = df_11.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_taxa).replace('.', ',').replace(',', '.', 1)
            total = df_11['taxa_total'].sum()
            total_taxa = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig11 = go.Figure()
            if toggle:
                fig11.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TOTAL TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_taxa}</span>"},
                                number_font={'color': 'white'},
                                value=0 
                ))
            else:
                fig11.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TOTAL TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_taxa}</span>"},
                                number_font={'color': '#303030'},
                                value=0 
                ))
            fig11.update_layout(main_config, height=170, template=template)
            fig11.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 11: {e}")
    return fig11

@app.callback(
     Output('graph12', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph12(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA']
            mask_status = df['status'].isin(status_incluidos)
            pix_in = ['PIX_IN']
            mask_pix_in = df['PIX_IN'].isin(pix_in)

            df_semanoemes = df.loc[(mask_status & mask_pix_in) & (df['fk_empresa'] == empresa_selecionada)]

            today = datetime.datetime.now()
            df_filtered_today = df_semanoemes[(df_semanoemes['DIA'] == today.day) & (df_semanoemes['MES'] == today.month) & (df_semanoemes['ANO'] == today.year)]
            df_12 = df_filtered_today.groupby('fk_empresa')['valor'].sum()
            transacoes_diaria = df_filtered_today.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_diaria).replace('.', ',').replace(',', '.', 1)
            df_12.sort_values(ascending=False, inplace=True)
            df_12 = df_12.reset_index()
            total = df_12['valor'].sum()
            total_cashin = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig12 = go.Figure()
            if toggle:
                fig12.add_trace(go.Indicator(
                            mode='number',
                            title={"text": f"<span style='font-size:80%; color:#1cb49c '>CASH-IN DIÁRIO</span><br><span style='font-size:70%; color:#1cb49c'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_cashin}</span>"},
                            number_font={'color': 'white'},
                            value=0
                ))
            else:
                fig12.add_trace(go.Indicator(
                            mode='number',
                            title={"text": f"<span style='font-size:80%; color:#1cb49c '>CASH-IN DIÁRIO</span><br><span style='font-size:70%; color:#1cb49c'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_cashin}</span>"},
                            number_font={'color': '#303030'},
                            value=0
                ))
            fig12.update_layout(main_config, height=170, template=template)
            fig12.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 12: {e}")
    return fig12

@app.callback(
     Output('graph13', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph13(month, year, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)

            df_filtered = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix) & (df['fk_empresa'] == empresa_selecionada)]

            df_13 = df_filtered[(df_filtered['status'] == 'CONCLUIDO') | (df_filtered['status'] == 'CONCLUIDA') | (df_filtered['status'] == 'processing') | (df_filtered['status'] == 'executed')]
            transacoes_menos_taxa = df_13.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_menos_taxa).replace('.', ',').replace(',', '.', 1)
            total = df_13['VALOR_MENOS_TAXA'].sum()
            total_menos_taxa = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig13 = go.Figure()
            if toggle:
                fig13.add_trace(go.Indicator(mode='number',
                            title={"text": f"<span style='font-size:80%'>TOTAL MENOS TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_menos_taxa}</span>"},
                            number_font={'color': 'white'}, 
                            value=0
                ))
            else:
                fig13.add_trace(go.Indicator(mode='number',
                            title={"text": f"<span style='font-size:80%'>TOTAL MENOS TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_menos_taxa}</span>"},
                            number_font={'color': '#303030'}, 
                            value=0
                ))
            fig13.update_layout(main_config, height=170, template=template)
            fig13.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 13: {e}")
    return fig13

@app.callback(
     Output('graph14', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph14(month_criacao, year_criacao, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter_criacao(year_criacao)
            mask_month = month_filter_criacao(month_criacao)
            mask_pix = pix_filter(pix_type)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed', 'ATIVA', 'CANCELADO']
            mask_status_pix = df['status'].isin(status_incluidos)

            df_filtered = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix) & (df['fk_empresa'] == empresa_selecionada)]
            df_14 = df_filtered[(df_filtered['status'] == 'CONCLUIDO') | (df_filtered['status'] == 'CONCLUIDA') | (df_filtered['status'] == 'processing') | (df_filtered['status'] == 'executed') | (df_filtered['status'] == 'ATIVA') | (df_filtered['status'] == 'CANCELADO')]
            
            
            transacoes_todos = df_14['valor'].count()
            transacoes_formatadas = "{:,.0f}".format(transacoes_todos).replace('.', ',').replace(',', '.', 1)
            fig14 = go.Figure()
            total = df_14['valor'].sum()
            total_criado = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            if toggle:
                fig14.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL CRIADO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_criado}</span>"},
                        number_font={'color': 'white'},
                        value=0
                ))
            else:
                fig14.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL CRIADO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_criado}</span>"},
                        number_font={'color': '#303030'},
                        value=0
                ))
            fig14.update_layout(main_config, height=170, template=template,)
            fig14.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 14: {e}")
    return fig14


@app.callback(
     Output('graph16', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-pix', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')],
    [State('authenticated-store', 'data')]
)
def update_graph16(month_criacao, year_criacao, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    global df_extrato_in
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter_criacao(year_criacao)
            mask_month = month_filter_criacao(month_criacao)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)

            df_filtered = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix) & (df['fk_empresa'] == empresa_selecionada)]

            df_16 = df_filtered[df_filtered['status'] == 'ATIVA']
            transacoes_ativas = df_16.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_ativas).replace('.', ',').replace(',', '.', 1)
            fig16 = go.Figure()
            total = df_16['valor'].sum()
            total_aberto = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)  
            if toggle:       
                fig16.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL ABERTO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_aberto}</span>"},
                        number_font={'color': 'white'},
                        value=0

                ))
            else:
                fig16.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL ABERTO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_aberto}</span>"},
                        number_font={'color': '#303030'},
                        value=0

                ))
            fig16.update_layout(main_config, height=170, template=template)
            fig16.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})


        except Exception as e:
            print(f"Erro ao atualizar gráficos 16: {e}")
    return fig16

@app.callback(
    Output('graph17', 'figure'),
    Input('radio-month', 'value'),
    Input('radio-year', 'value'),
    Input('radio-pix', 'value'),
    Input('radio-status-pix', 'value'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
    Input('interval-component', 'n_intervals'),
    [State('authenticated-store', 'data')]

)
def update_graphs(month_criacao, year_criacao, pix_type, status_list, toggle, n_intervals, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_year = year_filter_criacao(year_criacao)
            mask_month = month_filter_criacao(month_criacao)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list)

            df_filtered = df.loc[(mask_year & mask_month & mask_pix & mask_status_pix) & (df['fk_empresa'] == empresa_selecionada)]
            df_17 = df_filtered[df_filtered['status'] == 'CANCELADO']
            transacoes_canceladas = df_17.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_canceladas).replace('.', ',').replace(',', '.', 1)
            total = df_17['valor'].sum()
            total_expirado = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig17 = go.Figure()
            if toggle:
                fig17.add_trace(go.Indicator(
                    mode='number',
                    title={"text": f"<span style='font-size:80%; color:red'>TOTAL EXPIRADO</span><br><span style='font-size:70%; color:red'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:red';>R${total_expirado}</span>"},
                    value=0,
                    number_font={'color': 'white'}, 
            ))
            else:
                fig17.add_trace(go.Indicator(
                    mode='number',
                    title={"text": f"<span style='font-size:80%; color:red'>TOTAL EXPIRADO</span><br><span style='font-size:70%; color:red'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:red';>R${total_expirado}</span>"},
                    value=0,
                    number_font={'color': '#303030'}, 
                ))

            fig17.update_layout(main_config, height=170, template=template)
            fig17.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})
        except Exception as e:
            print(f"Erro ao obter dados dos graficos 17: {e}")
    return fig17


@app.callback(
    Output('table-container-in', 'children'),
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')],
    [State('authenticated-store', 'data')]
)
def update_table(toggle, n_intervals, start_date, end_date, authenticated_data):
    if not authenticated_data:
        raise PreventUpdate
    
    empresa_selecionada = authenticated_data
    
    try:
        template = template_theme1 if toggle else template_theme2
        
        df_extrato_in = cosultaextratoin(empresa_selecionada)

        df_extrato_in = df_extrato_in.rename(columns={
            'data_dia': 'Data',
            'valor_in': 'Transações Recebidas Cash-In',
            'qtd_in': 'Qtd de Transações Cash-In',
            'taxa_in': 'Taxa Total Cash-In',
            'menos_taxa_in': 'Total Menos Taxa Cash-In',
            'ticket_medio_in': 'Ticket Médio Cash-In',
            'valor_out': 'Transações Recebidas Cash-Out',
            'qtd_out': 'Qtd de Transações Cash-Out',
            'taxa_out': 'Taxa Total Cash-Out',
            'menos_taxa_out': 'Total Menos Taxa Cash-Out',
            'ticket_medio_out': 'Ticket Médio Cash-Out',
            'saldo_acumulado': 'Saldo Acumulado',
        })
        df_extrato_in = df_extrato_in[['Data', 'Transações Recebidas Cash-In', 'Qtd de Transações Cash-In', 'Taxa Total Cash-In', 'Total Menos Taxa Cash-In', 'Ticket Médio Cash-In', 'Saldo Acumulado', 'Transações Recebidas Cash-Out', 'Qtd de Transações Cash-Out', 'Taxa Total Cash-Out', 'Total Menos Taxa Cash-Out', 'Ticket Médio Cash-Out']]

        df_extrato_in['Data'] = pd.to_datetime(df_extrato_in['Data'])
        mask_in = (df_extrato_in['Data'] >= pd.to_datetime(start_date)) & (df_extrato_in['Data'] <= pd.to_datetime(end_date))
        df_extrato_in = df_extrato_in.loc[mask_in].sort_values('Data').copy()

        df_extrato_in = df_extrato_in.sort_values(by='Data', ascending=False)

        df_extrato_in = df_extrato_in.fillna(0)
    
        df_extrato_in['Data'] = df_extrato_in['Data'].dt.strftime('%d/%m/%Y')

        data_combined = []
        for idx, row in df_extrato_in.iterrows():
            data_combined.extend([
                {'Descrição': 'Cash-In:', 'Valor': row['Data']},
                {'Descrição': 'Recebido Cash-In', 'Valor': '{:,.2f}'.format(row['Transações Recebidas Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Qtd. Cash-In', 'Valor': '{:,.0f}'.format(row['Qtd de Transações Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Taxa Cash-In', 'Valor': '{:,.2f}'.format(row['Taxa Total Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Menos a Taxa Cash-In', 'Valor': '{:,.2f}'.format(row['Total Menos Taxa Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Ticket Médio Cash-In', 'Valor': '{:,.2f}'.format(row['Ticket Médio Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Cash-Out:', 'Valor': ''},
                {'Descrição': 'Pago Cash-Out', 'Valor': '{:,.2f}'.format(row['Transações Recebidas Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Qtd. Cash-Out', 'Valor': '{:,.0f}'.format(row['Qtd de Transações Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Taxa Cash-Out', 'Valor': '{:,.2f}'.format(row['Taxa Total Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Menos a Taxa Cash-Out', 'Valor': '{:,.2f}'.format(row['Total Menos Taxa Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Ticket Médio Cash-Out', 'Valor': '{:,.2f}'.format(row['Ticket Médio Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Saldo Acumulado', 'Valor': '{:,.2f}'.format(row['Saldo Acumulado']).replace(',', '|').replace('.', ',').replace('|', '.'), 'id': 'saldo-acumulado'},
                {'Descrição': '--------------------', 'Valor': '----------'}
            ])

        table_combined = dash_table.DataTable(
            id='table-combined',
            columns=[{"name": i, "id": i} for i in ['Descrição', 'Valor']],
            data=data_combined,
            style_table={'overflowX': 'scroll', 'width': '100%', 'margin': 'auto', 'border':'1px solid gray'},
            style_cell={'textAlign': 'left', 'padding': '5px', 'whiteSpace': 'normal', 'height': 'auto', 'border-top': 'none', 'border-right': 'none'},
            style_header={'fontWeight': 'bold', 'backgroundColor': 'white', 'color': 'black', 'text-align': 'center', 'border-bottom':'1px solid gray', 'margin-top':'50px'},
            style_data={'whiteSpace': 'normal', 'height': 'auto', 'color': 'black', 'backgroundColor': 'white'},
            export_format='csv',
            style_data_conditional=[
            {
                'if': {'column_id': 'Valor'},
                'textAlign': 'right',
            },
            {

                'if': {'column_id': 'Valor', 'filter_query': '{id} eq "saldo-acumulado"'},
                'color': '#1cb49c',
                'fontWeight': 'bold'
            },
            {
                'if': {'column_id': 'Descrição', 'filter_query': '{id} eq "saldo-acumulado"'},
                'color': '#1cb49c',
                'fontWeight': 'bold'
            }
        ]
        )
        
        return table_combined

    except Exception as e:
        print(f"Erro ao obter dados da tabela: {e}")
        return html.Div(f"Erro ao carregar os dados da tabela: {e}")


@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')],
    [State('authenticated-store', 'data')]
)
def display_page(pathname, empresa_selecionada):
    if empresa_selecionada:
        if pathname.startswith('/main_layout/'):
            return main_layout
        else:
            return login_layout
    else:
        return login_layout

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),
    dcc.Store(id='authenticated-store', storage_type='session', data=None)
])

@app.callback(
    Output('logout-output', 'children'),
    [Input('logout-button', 'n_clicks')],
    [State('url', 'pathname')]
)
def update_output(n_clicks, pathname):
    if n_clicks is None:
        raise PreventUpdate
    return dcc.Location(pathname='/', id='main_layout_redirect')


mode = 'prod'

if __name__ == '__main__':
    if mode == 'dev':
        app.run(host='0.0.0.0', port='8050')
    else:
        serve(app.server, host='0.0.0.0', port='8051', threads=30)