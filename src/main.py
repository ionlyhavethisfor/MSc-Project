# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 13:37:52 2024.

@author: caspe
"""

from src.funcs import unzip_store, normalise, get_batch, remove_trailing_brackets, generate_trace
import math
import zlib
import pickle
import base64
from flask_caching import Cache
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import Dash, html, Input, Output, dcc, State, ctx, ALL, no_update, dash_table
import json
import gc
import time
from dash_holoniq_wordcloud import DashWordcloud
from dateutil import parser
from keybert import KeyBERT
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('punkt_tab')

# =============================================================================
# Loading database, geojson, etc.
# =============================================================================

db_name = 'databases/nov18.db'


with open('maps/map.geojson', mode='r', encoding='utf-8') as f:
    geojson_data = json.load(f)
geojson_df = pd.DataFrame([{
    "NAME": feature["properties"].get("NAME", None),  # Property field in GeoJSON
    "PART": feature["properties"].get("PARTOF", None),  # Property field in GeoJSON
    "SUBJ": feature["properties"].get("SUBJECTO", None),  # Property field in GeoJSON
} for feature in geojson_data['features']])

kw_model = KeyBERT()

class SQLiteConnection:
    """Use for database queries."""

    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(f'file:{self.db_name}?mode=ro', uri=True)
        self.cursor = self.conn.cursor()
    def __enter__(self):
        self.cursor.execute("PRAGMA synchronous = OFF;")
        self.cursor.execute("PRAGMA cache_size = -50000;")
        self.cursor.execute("PRAGMA temp_store = MEMORY;")
        self.cursor.execute("PRAGMA optimizer_pragmas = 'all';")
        return self.conn, self.cursor
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()


# %% Metadata options for chart 1

aggregate_options = [
    {'label': 'Gender', 'value': 'Gender'},
    {'label': 'Birth Country', 'value': 'CountryOfBirth'},
    {'label': 'Date of Birth', 'value': 'DateOfBirth'},
    {'label': 'Interview Language', 'value': 'LanguageLabel'},
    {'label': 'Experience', 'value': 'ExperienceGroup'},
]

# %% Dictionary for Experience Group dropdown.
with SQLiteConnection(db_name) as (conn, cursor):
    query = """
        SELECT ExperienceGroup, COUNT(DISTINCT PIQPersonID) as count
        FROM BioTable
        GROUP BY ExperienceGroup
        ORDER BY count DESC
    ;"""
    cursor.execute(query,)
    exp_group_list = cursor.fetchall()

exp_group_listdict = [{"label": f"{tup[0]}, {tup[1]} entries", "value": tup[0]} for tup in exp_group_list]


# %% Table that accompanies tag cloud. Filled with dummy data here.
with SQLiteConnection(db_name) as (conn, cursor):
    query = """
    SELECT DISTINCT KeywordLabel, COUNT(*) as count, ParentLabel, RootLabel
    FROM KeywordsTable
    GROUP BY KeywordLabel
    ORDER BY count DESC
    LIMIT 20
    ;
    """
    datatable_alldata_df = pd.read_sql_query(query, conn)

global_table = dash_table.DataTable(
        id='datatable',
        columns=[{"name": i, "id": i} for i in datatable_alldata_df.columns],
        data=datatable_alldata_df.to_dict('records'),
        page_size=20,
        sort_action='native',
        sort_mode="multi",
        filter_action="native",
        fixed_rows={'headers': True},
        cell_selectable=True,
        style_table={'overflow': 'auto', 'height': '20vh'},
        style_cell={'textAlign': 'left', 'fontSize': '1vh', 'width': '5vw',
                    'whiteSpace': 'pre-line',
                    'wordBreak': 'break-all',
                    'overflowWrap': 'break-word'},
        style_data_conditional=[{
            'if': {'row_index': 'even'},
            'backgroundColor': 'rgb(220, 220, 220)'
        }],
        style_header={
            'backgroundColor': 'lightgrey',
            'fontWeight': 'bold'
        },
)
# %% birthplaces dataframe. Used for map.
with SQLiteConnection(db_name) as (conn, cursor):
    query = """
    SELECT DISTINCT
        BioTable.PIQPersonID,
        Subquery.KeywordLabel,
        BioTable.CityOfBirth,
        Subquery.Latitude,
        Subquery.Longitude
    FROM BioTable
    LEFT JOIN (
        SELECT DISTINCT
            KeywordLabel,
            Latitude,
            Longitude
        FROM KeywordsTable
        WHERE Latitude IS NOT NULL
    ) AS Subquery
    ON BioTable.CityOfBirth = Subquery.KeywordLabel
    """
    birth_df_raw = pd.read_sql_query(query, conn)

birth_df = birth_df_raw.groupby([
    'KeywordLabel', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)
# %% Getting all locations, and function for getting answers.

with SQLiteConnection(db_name) as (conn, cursor):
    query = """
    SELECT DISTINCT
        KeywordsTable.KeywordLabel, KeywordsTable.Latitude, KeywordsTable.Longitude
    FROM KeywordsTable
    WHERE
        KeywordsTable.Latitude IS NOT NULL
    AND
        KeywordsTable.Longitude IS NOT NULL
    ;
    """
    coordinates_df = pd.read_sql_query(query, conn)


def get_answer(q):
    """Use for counting answers by question."""
    with SQLiteConnection(db_name) as (conn, cursor):
        query = f"""
            SELECT DISTINCT PIQPersonID, Answer
            FROM QuestionsTable
            WHERE QuestionText = '{q}'
        ;
        """
        df = pd.read_sql_query(query, conn)
    return df
# %% Hiding place locations

def get_coords_byquestion(question):
    """Get the coordinates of keywords depending on the question input."""
    with SQLiteConnection(db_name) as (conn, cursor):
        query = f"""
            SELECT DISTINCT Q.PIQPersonID, Answer, Latitude, Longitude
            FROM QuestionsTable Q
            LEFT JOIN KeywordsTable K ON Answer = KeywordLabel
            WHERE
                Latitude IS NOT NULL
                AND QuestionText = '{question}'
        ;
        """
        return pd.read_sql_query(query, conn)


hiding_df = get_coords_byquestion('Hiding or Living under False Identity (Location)')
hiding_df_grouped = hiding_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)


# %% liberation location


liberation_df = get_answer('Location of Liberation')
liberation_df = pd.merge(
                    liberation_df,
                    coordinates_df,
                    left_on='Answer',
                    right_on='KeywordLabel',
                    how='left'
                    )
liberation_df = liberation_df.dropna()

liberation_df_grouped = liberation_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)

# %% Ghettos

ghettos_df = get_answer("Ghetto(s)")
ghettos_df = pd.merge(ghettos_df, coordinates_df,
                      left_on='Answer',
                      right_on='KeywordLabel',
                      how='left')
ghettos_df = ghettos_df.dropna()

ghettos_df_grouped = ghettos_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)
# %% camps
# Same as above but for all camps which will then be segmented further.
allcamps_df = get_answer("Camp(s)")
allcamps_df = pd.merge(allcamps_df, coordinates_df,
                       left_on='Answer',
                       right_on='KeywordLabel',
                       how='left')
allcamps_df = allcamps_df.dropna()

# camp types?
deathcamps_df = allcamps_df[allcamps_df["KeywordLabel"].str.contains(
    "Death Camp", na=False, case=False)]
concamps_df = allcamps_df[allcamps_df["KeywordLabel"].str.contains(
    r"Concentration Camp|Concentation Camp", na=False, case=False)]
interncamps_df = allcamps_df[allcamps_df["KeywordLabel"].str.contains(
    r"Internment Camp|internment Camp", na=False, case=False)]
powcamps_df = allcamps_df[allcamps_df["KeywordLabel"].str.contains(
    r"POW Camp|POW", na=False, case=False)]


deathcamps_df_grouped = deathcamps_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)

concamps_df_grouped = concamps_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)

interncamps_df_grouped = interncamps_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)

powcamps_df_grouped = powcamps_df.groupby([
    'Answer', 'Latitude', 'Longitude'
]).size().reset_index(name='count').sort_values(by='count', ascending=False)

# %%
with SQLiteConnection(db_name) as (conn, cursor):
    query = """
    SELECT DISTINCT CountryOfBirth
    FROM BioTable
    ;"""
    countries = pd.read_sql_query(query, conn).dropna()
countries = countries["CountryOfBirth"].tolist()


# %%
# =============================================================================
# Mainly stuff related to styles.
# There is also styles directly in the code, as well as a styles.css file.
# It is a mess, and I apologize.
# =============================================================================
color_scheme = "#F5F5F5"
color_scheme_secondary = '#1a1a1a'  # '#593196' # "#272b30" # "#325d88"
dd_background = {
    'color': '#1a1a1a',
    'backgroundColor': color_scheme
    }
button_color = 'red'

colors = px.colors.qualitative.Dark2

b1_col, b2_col, b3_col, b4_col, b5_col, b6_col, b7_col, b8_col = colors

accordion_style = {
    'backgroundColor': color_scheme, 'borderColor': color_scheme}

default_b_style = {'flex': 1,
                   "boxSizing": "border-box", "fontSize": "1.5vh"}

container_style = {"height": "100vh", "width": "100vw", 'margin': 0, 'padding': '0', 'position': 'relative'}

graph_style = {'flexGrow': 1, 'backgroundColor': color_scheme,
               "margin": "0", "border": "None"}
dropdown_style = {
    "padding": "0",
    'minWidth': '13vw',
    "width": "auto", "height": "auto", "fontSize": "1.5vh"
    }
dropdown_container_style = {"width": "100vw", "height": "10vh", 'backgroundColor': color_scheme}

spinnerstyle = {'height': '10rem', 'width': '10rem'}


# %%

app = Dash(__name__,
           suppress_callback_exceptions=True,
           external_stylesheets=[dbc.themes.LUX, dbc.icons.BOOTSTRAP],
           meta_tags=[
               {"name": "viewport", "content": "width=device-width, initial-scale=1"},
           ],
           )

server = app.server

cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory',
    'CACHE_DEFAULT_TIMEOUT': 300
})


@cache.memoize()    # Saves some of the results. Helps speed up common queries that are a little slow.
def data_query(yb, li, ki, ai, ti, lang, gend, exp, cntry, online):
    """Use for the filter_store large query."""
    with SQLiteConnection(db_name) as (conn, cursor):
        query = f"""
            {yb}
            {li}
            {ki}
            {ai}
            {ti}
            SELECT PIQPersonID
            FROM BioTable
            WHERE LanguageLabel IN {lang}{gend}{exp}{cntry}{online}
                ;"""
        cursor.execute(f"EXPLAIN QUERY PLAN {query}")
        # query_plan = cursor.fetchall()
        # for step in query_plan:
        #     print(step)
        cursor.execute(query)
        results = cursor.fetchall()
    return results


app.layout = dbc.Container([
    dcc.Store(id='selected_piq_store'),
    dbc.Offcanvas(
            html.Div([
                html.Div(id="testimony_selectors"),
                html.Div(
                    DashWordcloud(
                        id='wordcloud',
                        list=[['placeholder', 2]],
                        width=1,
                        height=1,
                        gridSize=16,
                        color="white",
                        backgroundColor=color_scheme_secondary,
                        shuffle=True,
                        rotateRatio=0,
                        shrinkToFit=True,
                        shape='circle',
                        hover=True,
                        style={"borderRadius": "2vh"}
                    ),
                    id="testimony_area",
                    style={
                        "fontSize": "2vh",
                        "maxHeight": "100%",
                        "paddingBottom": "10vh"
                    })
            ]),

            id="offcanvas",
            title=html.Img(
                src="https://memorise.sdu.dk/wp-content/uploads/2022/10/cropped-logoOctober-1.png",
                style={
                    'height': '10vh',
                    'width': 'auto',
                    'display': 'block'
                },
                alt='Memorise organization logo.'
            ),
            backdrop=False,
            is_open=False,
            style={'width': '83vw', 'backgroundColor': color_scheme}
        ),
    dbc.Row([
        dbc.Col([
            dbc.Row(
                dbc.Stack([
                    html.Img(
                        src="https://memorise.sdu.dk/wp-content/uploads/2022/10/cropped-logoOctober-1.png",
                        style={
                            'height': '10vh',
                            'width': 'auto',
                            'display': 'block'
                        },
                        alt='Memorise organization logo.'
                    ),
                    dbc.Spinner([
                        dbc.Table(html.Tbody([], id='counter', style={'fontSize': '1.5vh'}),
                                  borderless=True,
                                  style={'textAlign': 'center', 'width': '100%', 'marginLeft': '0'},
                                  size='sm'),
                        dcc.Store(id="filter_store")
                    ]),

                    dbc.Stack([
                        dbc.RadioItems(
                            options=[
                                {"label": html.Div([html.I(className="bi bi-gender-male"), "Men"]), "value": 'Male'},
                                {"label": html.Div([html.I(className="bi bi-gender-female"), "Women"]), "value": 'Female'},
                                {"label": html.Div([html.I(className="bi bi-gender-ambiguous"), "Any"]), "value": 'Any'}
                            ],
                            value='Any',
                            id="gender_dd",
                            inline=True,
                        ),

                    ], direction='horizontal', gap=1, style=dd_background),

                    dbc.Stack([
                        html.I(className="bi bi-translate"),
                        dbc.Checklist(
                            options=[
                                {"label": "English", "value": "English"},
                                {"label": "German", "value": "German"},
                                {"label": "Czech", "value": 'Czech'},
                                {"label": "Dutch", "value": 'Dutch'},
                            ],
                            value=['English', 'German', 'Czech', 'Dutch'],
                            id="language_dd",
                            inline=True
                        ),
                    ], direction='horizontal', gap=1, style=dd_background),

                    dbc.Stack([
                        html.I(className="bi bi-camera-reels"),
                        dbc.Switch(
                            id="online_dd",
                            label=html.Span([
                                "Video in the ",
                                html.A("Visual History Archive", href="https://vha.usc.edu/", target="_blank"),
                                "?"
                            ]),
                            value=False,
                        ),
                    ], direction='horizontal', gap=1),

                    dbc.Stack([
                        html.I(className="bi bi-calendar-date"),
                        dbc.Col([
                            dbc.Label(id='range_slider_values', style={'marginLeft': '2vw'}),
                            dcc.RangeSlider(min=1892, max=1945, step=1, value=[1892, 1945],
                                            id='yearborn_dd', marks=None
                                            ),
                            ], width={'width': 12, 'offset': 0}, style={'justifyContent': 'center'})
                    ], direction='horizontal', gap=2, style=dd_background),

                    dbc.Stack([
                        html.I(className="bi bi-person-fill-exclamation"),
                        dcc.Dropdown(
                            id="experience_dd",
                            className="dropdown",
                            options=exp_group_listdict,
                            value='Jewish Survivor',
                            style=dropdown_style,
                            optionHeight=100,
                            # maxHeight=400,
                            placeholder="Define experience background"
                        ),
                        dbc.Tooltip("""
                                    Survivors can be categorized according to their lived experience.
                                    The largest group is 'Jewish Survivors' (of the Holocaust).
                                    Others may have been witnesses, or their experiences may relate
                                    to other atrocities such as the Armenian genocide.
                                    """, target='experience_dd')
                    ], direction='horizontal', gap=1),

                    dbc.Stack([
                        html.I(className="bi bi-pin-map"),
                        dcc.Dropdown(
                            id="locations_dd",
                            className="dropdown",
                            options=[],
                            value=None,
                            style=dropdown_style,
                            placeholder="Click map to define locations",
                            searchable=True
                        ),
                        dbc.Tooltip("""Physical locations. On the map they appear if mentioned
                                    in the questionnaires of survivors. You can click on the map,
                                    or search here.
                                    """, target='locations_dd')
                    ], direction='horizontal', gap=1),

                    dbc.Stack([
                        html.I(className="bi bi-flag-fill"),
                        dcc.Dropdown(
                            id="country_dd",
                            className="dropdown",
                            options=countries,
                            multi=True,
                            value=[],
                            style=dropdown_style,
                            optionHeight=50,
                            # maxHeight=300,
                            placeholder="Select country of origin"
                        ),

                        ], direction='horizontal', gap=1),

                    dbc.Stack([
                        html.I(className="bi bi-question-square-fill"),
                        dcc.Dropdown(
                            id="answer_dd",
                            className="dropdown",
                            options=[],
                            value=None,
                            multi=True,
                            style=dropdown_style,
                            optionHeight=100,
                            placeholder="Filter by answers to questions",
                            searchable=True
                        ),
                        dbc.Tooltip("""
                                    Interviewees were asked standardized questions.
                                    Search here, or interact with one of the graphs,
                                    to search based on question and answer combinations.
                                    """, target='answer_dd')
                        ], direction='horizontal', gap=1),
                    dbc.Stack([
                        html.I(className="bi bi-chat-right-text"),
                        dcc.Dropdown(
                            id="keyword_dd",
                            className="dropdown",
                            options=[],
                            value=[],
                            multi=True,
                            style=dropdown_style,
                            optionHeight=75,
                            # maxHeight=300,
                            placeholder="Search by keyword."
                        ),
                        dbc.Tooltip("""'Keywords' are thematic tags applied to the testimonies.
                                    Search here or interact with the wordcloud to filter by keywords.
                                    """, target='keyword_dd')
                        ], direction='horizontal', gap=1),
                    dbc.Stack([
                        html.I(className="bi bi-book"),
                        dbc.Stack([
                            dbc.Input(
                                id='search_in_testimony_input',
                                placeholder="Search in testimonies. Example: 'coffee'",
                                type="search",
                                debounce=True,
                                autocomplete='off',
                                inputmode='latin-name',
                                minlength='3',
                                autofocus=True,
                                style=dropdown_style,
                            ),
                            dcc.Dropdown(
                                id="testimony_dd",
                                className="dropdown",
                                options=[],
                                value=None,
                                multi=True,
                                style=dropdown_style,
                                placeholder='Search terms appear here.',
                                searchable=False
                            ),
                            dbc.Tooltip("""Use the above input bar to search for terms
                                        within the testimonies. Try something broad, like 'coffee',
                                        or something more specific.
                                        """, target='testimony_dd')
                            ])
                        ], direction='horizontal', gap=1),
                    ], gap=2, direction='vertical', style={'height': '100vh', 'overflowY': 'auto'}),
                style={'height': '100%', 'backgroundColor': color_scheme, 'paddingLeft': '1vh'}),
            ], width=2),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Button('Birthplaces', id='birth', n_clicks=0, style=default_b_style),
                    dbc.Button('Hidingplaces', id='hiding', n_clicks=0, style=default_b_style),
                    dbc.Button('Liberation Locations', id='liber', n_clicks=0, style=default_b_style),
                    dbc.Button('Internment Camps', id='intern', n_clicks=0, style=default_b_style),
                    dbc.Button('Prisoner of War Camps', id='pow', n_clicks=0, style=default_b_style),
                    dbc.Button('Ghettos', id='ghetto', n_clicks=1, style=default_b_style),
                    dbc.Button('Concentration Camps', id='concen', n_clicks=1, style=default_b_style),
                    dbc.Button('Extermination Camps', id='death', n_clicks=0, style=default_b_style),
                    ], width=12, style={'height': '7vh', 'display': 'flex', 'justifyContent': 'center'})
            ]),
            dbc.Row([
                dbc.Toast(id='place_info', is_open=False,
                          style={'position': 'absolute',
                                 'bottom': 0, 'left': 0,
                                 'width': 'auto', 'zIndex': 2,
                                 'color': 'black'}
                          ),
                dcc.Graph(id="map",
                          style={'height': '100%', 'width': '100%',
                                 'margin': 0, 'padding': 0,
                                 'backgroundColor': color_scheme},
                          config={'displayModeBar': False, 'scrollZoom': True}, responsive=True,),
            ], style={'height': '63vh', 'width': '100%', 'margin': 0,
                      'padding': 0, 'backgroundColor': color_scheme,
                      'position': 'relative'}),
            dbc.Row([
                dbc.Col([
                    dbc.Col([
                        dcc.Dropdown(
                            id="aggregate_dd",
                            className="dropdown",
                            options=aggregate_options,
                            clearable=False,
                            value='Gender',
                            style={"width": "100%"},
                            placeholder="Get aggregate data about filtered group.."
                        )], width={"size": 10, "offset": 1},
                    ),
                    
                    dcc.Graph(
                        id='aggregate_graph',
                        style={
                          "height": "25vh",
                          'backgroundColor': color_scheme,
                          'borderColor': color_scheme},
                        config={'displayModeBar': False,
                                'scrollZoom': True,
                                'doubleClick': 'reset',
                                'showAxisDragHandles': True},
                        responsive=True,
                        ),
                    
                    ], style=accordion_style),
                dbc.Col([
                    dbc.Col([
                        dcc.Dropdown(
                            id="question_dd",
                            className="dropdown",
                            options=[],
                            value='Ghetto(s)',
                            clearable=False,
                            style={"width": "100%"},
                            optionHeight=50,
                            # maxHeight=300,
                            placeholder="No questionnaire available for the selected group.",
                        ),
                        ], width={"size": 10, "offset": 1}
                    ),

                        dcc.Graph(
                        id="questionnaire_graph",
                        style={
                            "height": "25vh",
                            'backgroundColor': color_scheme,
                            'borderColor': color_scheme},
                        config={'displayModeBar': False,
                                'scrollZoom': True,
                                'doubleClick': 'reset',
                                'showAxisDragHandles': True},
                        responsive=True
                    )
                ], style=accordion_style),
                dbc.Col([
                    dbc.Tabs([
                        dbc.Tab(
                            html.Div(id='tab1'),
                            label="Keyword Cloud",
                            tab_style={'border': '1px solid black'},
                            active_label_style={"color": "white", 'backgroundColor': color_scheme_secondary}
                            ),
                        dbc.Tab(global_table, label="Keyword Table", tab_style={'border': '1px solid black'},
                                active_label_style={"color": "white", 'backgroundColor': color_scheme_secondary}),
                        ], style={'minWidth': '100%', 'height': '3vh', 'fontSize': '1vh'},)
                ], style=accordion_style),
            ], style={'height': '30vh', 'overflow': 'hidden'}),

            ], width=8, style={'backgroundColor': color_scheme}),
        dbc.Col([
            dbc.Stack([
                dbc.Input(
                    id='name-search-input',
                    placeholder='Search ALL survivors..',
                    type="search",
                    debounce=True,
                    autocomplete='off',
                    inputmode='latin-name',
                    minlength='3',
                          ),
                dbc.Button('Open canvas', disabled=True, id='canvas_button', size='sm'),
                html.Div(id="list_of_people",
                            style={
                                'display': 'flex',
                                'justifyContent': 'flex-start',
                                'flexWrap': 'wrap',
                                "height": "40vh"},),
                dbc.Stack([
                    dbc.Pagination(id="pagination", max_value=10, size="sm", fully_expanded=False, style={
                                    "height": "auto", 'maxWith': '100%'}, previous_next=True),
                    ], direction='horizontal', style={'position': 'absolute', 'bottom': 0, 'marginBottom': 0})
                ], style={'height': '100%',
                          'backgroundColor': color_scheme,
                          'backgroundColor': color_scheme,
                          'margin': 0, 'padding': 0}
                ),
            ], width=2, style={'backgroundColor': color_scheme}),
        ], style={'width': '100vw'}),

], style=container_style)


@app.callback(
    Output("filter_store", "data"),
    Input("gender_dd", "value"),
    Input("experience_dd", "value"),
    Input("country_dd", "value"),
    Input("language_dd", "value"),
    Input("keyword_dd", "value"),
    Input("locations_dd", "value"),
    Input("yearborn_dd", "value"),
    Input("answer_dd", "value"),
    Input('online_dd', 'value'),
    Input('testimony_dd', 'value'),
)
def storing_func(gend, exp, cntry, lang, key, locations, yearborn, answer, online, testimony):
    """Use for storing query results (PIQ) for use in other components."""
    start = time.time()
    if yearborn:
        if yearborn[0] == 1892 and yearborn[1] == 1945:
            yearborn = ""
        else:
            yb_list = [f"DateOfBirth LIKE '%{year}%'" for year in range(yearborn[0], yearborn[1], 1)]
            yearborn = 'SELECT PIQPersonID FROM BioTable WHERE ' + ' OR '.join(yb_list) + ' INTERSECT'
    else:
        yearborn = ""

    if testimony:
        if len(testimony) > 1:
            testimony = [f'"{t}"' for t in testimony]
            testimony = ' AND '.join(testimony)
            testimony_intersect = f"""
                SELECT PIQPersonID
                FROM TestimonyTable_fts
                WHERE TestimonyTable_fts MATCH '{testimony}'
                INTERSECT
            """
        else:
            testimony_intersect = f"""
                SELECT PIQPersonID
                FROM TestimonyTable_fts
                WHERE TapeTestimony MATCH '"{testimony[0]}"'
                INTERSECT
            """
    else:
        testimony_intersect = ''

    if online:
        online = " AND InVHAOnline = 'True'"
    else:
        online = ""

    answer_intersect = ""
    if answer:
        answer = [f"""QuestionText = '{a.split(': ', 1)[0]}' AND Answer = "{a.split(': ', 1)[1]}" """ for a in answer]
        if len(answer) > 1:
            answer = [f'SELECT PIQPersonID FROM QuestionsTable WHERE {a}' for a in answer]
            answer_intersect = " INTERSECT ".join(answer) + ' INTERSECT '
        else:
            answer = answer[0]
            answer_intersect = f"""
                SELECT PIQPersonID
                FROM QuestionsTable
                WHERE {answer}
            INTERSECT
            """
    locations_intersect = ""

    if locations:
        locations_intersect = f"""
            SELECT PIQPersonID
            FROM QuestionsTable
            WHERE
                QuestionText = 'Camp(s)'
                AND Answer = "{locations}"
            UNION
            SELECT PIQPersonID
            FROM QuestionsTable
            WHERE
                QuestionText = 'Ghetto(s)'
                AND Answer = "{locations}"

            UNION

            SELECT PIQPersonID
            FROM BioTable
            WHERE CityOfBirth = "{locations}"

            UNION

            SELECT PIQPersonID
            FROM QuestionsTable
            WHERE
                QuestionText = 'Location of Liberation'
                AND Answer = "{locations}"
            UNION

            SELECT PIQPersonID
            FROM QuestionsTable
            WHERE
                QuestionText = 'Hiding or Living under False Identity (Location)'
                AND Answer = "{locations}"

            INTERSECT
        """

    if gend == 'Male' or gend == 'Female':
        gend = f" AND Gender = '{gend}'"
    else:
        gend = ""

    if exp:
        exp = f' AND ExperienceGroup = "{exp}"'
    else:
        exp = ""
    if cntry:
        if len(cntry) == 1:
            cntry = f" AND CountryOfBirth IN ('{cntry[0]}')"
        else:
            cntry = f" AND CountryOfBirth IN {tuple(cntry)}"
    else:
        cntry = ""
    if lang:
        if len(lang) > 1:
            lang = tuple(lang)
        else:
            lang = f"('{lang[0]}')"
    else:
        lang = "() "

    if key:
        key = [f"""KeywordID = "{k}" """ for k in key]
        if len(key) > 1:
            key = [f'SELECT PIQPersonID FROM KeywordsTable WHERE {k}' for k in key]
            keyword_intersect = " INTERSECT ".join(key) + ' INTERSECT '
        else:
            key = key[0]
            keyword_intersect = f"""
                SELECT PIQPersonID
                FROM KeywordsTable
                WHERE {key}
            INTERSECT
            """
    else:
        keyword_intersect = ""
    start = time.time()
    results = data_query(
        yearborn, locations_intersect, keyword_intersect,
        answer_intersect, testimony_intersect,
        lang, gend, exp, cntry, online)
    print(f'filter_store callback took {time.time() - start:.2f} seconds')
    results = [r[0] for r in results]

    results = zlib.compress(pickle.dumps(results))
    results = base64.b64encode(results).decode('utf-8')
    gc.collect()
    return results


@app.callback(
    Output("questionnaire_graph", "figure"),
    State("filter_store", "data"),
    Input("question_dd", "value"),
    Input('aggregate_dd', 'value'),
    Input('aggregate_graph', 'figure'),
    prevent_initial_call=True
)
def generate_questionnaire_graph(someinput, quest, agg_val, agg_fig):
    """Update the questionnaire graph."""
    if agg_fig is None:
        return no_update
    if someinput and agg_fig:
        someinput = unzip_store(someinput)
        with SQLiteConnection(db_name) as (conn, cursor):
            if agg_val != 'DateOfBirth':
                query = f"""
                        SELECT Answer, COUNT(*) AS count, {agg_val}
                        FROM QuestionsTable Q
                        LEFT JOIN BioTable B
                        ON Q.PIQPersonID = B.PIQPersonID
                        WHERE QuestionText = '{quest}' AND Q.PIQPersonID IN {someinput}
                        GROUP BY Answer, {agg_val}
                        ;"""
            else:
                agg_val = 'Answer'
                query = f"""
                        SELECT Answer, COUNT(*) AS count
                        FROM QuestionsTable
                        WHERE QuestionText = '{quest}' AND PIQPersonID IN {someinput}
                        GROUP BY Answer
                        ;"""
            df = pd.read_sql_query(query, conn)

        if agg_fig['data'][0]['marker']['color'] != color_scheme_secondary:
            colors = {trace['name']: trace['marker']['color'] for trace in agg_fig['data']}
        else:
            agg_val = 'Answer'
            colors = {d: color_scheme_secondary for d in df['Answer']}
        df['Answer_clean'] = df['Answer'].apply(remove_trailing_brackets)

        fig = px.bar(
            df,
            y="count",
            x="Answer",
            color_discrete_map=colors,
            color=agg_val,
        )
        fig.update_xaxes(
            ticktext=df["Answer_clean"],
            tickvals=df["Answer"],
        )

        fig.update_layout(
            barmode='stack', xaxis={'categoryorder': 'total descending'},
            hovermode="closest",
            hoverlabel={
                'bgcolor': color_scheme_secondary,
                'font': {'size': 16, 'color': 'white'}
                # 'font_size': 6,
            }
        )
        fig.update_layout(
            xaxis={"range": [-.5, min(df['Answer'].nunique(), 7.5)]},
            paper_bgcolor='#EDEDED',
            plot_bgcolor='rgba(0,0,0,0)',
            autosize=True,
            margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
            xaxis_title="",
            yaxis_title="",
            dragmode='pan',
            showlegend=False

        )
        fig.update_traces(
            hovertemplate='%{y}<br>%{fullData.name}<extra></extra>'
        )
        return fig


@app.callback(
    Output("map", "figure"),
    Input('birth', 'n_clicks'),
    Input('intern', 'n_clicks'),
    Input('pow', 'n_clicks'),
    Input('ghetto', 'n_clicks'),
    Input('concen', 'n_clicks'),
    Input("death", "n_clicks"),
    Input('liber', 'n_clicks'),
    Input('hiding', 'n_clicks'),
    Input('filter_store', 'data'),
    State("map", "figure"),
    prevent_initial_call=True
)
def make_map(b1, b2, b3, b4, b5, b6, b7, b8, filterdata, relayout):
    """Update the map."""
    if filterdata:
        filterdata = unzip_store(filterdata)
    if relayout:
        projection = relayout['layout']['map']['zoom']
        centering = relayout['layout']['map']['center']
    else:
        centering = {'lat': 49, 'lon': 15}
        projection = 3.7

    fig = go.Figure(go.Choroplethmap(
        geojson=geojson_data,
        locations=geojson_df['NAME'],
        z=[1]*len(geojson_data['features']),
        featureidkey="properties.NAME",
        colorscale=[[0, 'rgba(0, 0, 0, 0)'], [1, 'rgba(0, 0, 0, 0)']],
        hoverinfo='skip',
        marker={'opacity': .5},
        marker_line_width=1.5,
        marker_line_color='black',
        showscale=False,
    ))

    fig.update_layout(
        map_style='carto-voyager-nolabels',
        map_center=centering,
        map_zoom=projection,
        margin=dict(
            l=0,
            r=0,
            t=0,
            b=0
        ),
        showlegend=False,
    )

    if b1 % 2 != 0:
        trace = generate_trace(filterdata, birth_df_raw, b1_col, 'KeywordLabel', False)
        fig.add_trace(trace)
    if b2 % 2 != 0:
        trace = generate_trace(filterdata, interncamps_df, b2_col, 'Answer', False)
        fig.add_trace(trace)
    if b3 % 2 != 0:
        trace = generate_trace(filterdata, powcamps_df, b3_col, 'Answer', False)
        fig.add_trace(trace)
    if b4 % 2 != 0:
        trace = generate_trace(filterdata, ghettos_df, b4_col, 'Answer', False)
        fig.add_trace(trace)
    if b5 % 2 != 0:
        trace = generate_trace(filterdata, concamps_df, b5_col, 'Answer', False)
        fig.add_trace(trace)
    if b6 % 2 != 0:
        trace = generate_trace(filterdata, deathcamps_df, b6_col, 'Answer', False)
        fig.add_trace(trace)
    if b7 % 2 != 0:
        trace = generate_trace(filterdata, liberation_df, b7_col, 'Answer', False)
        fig.add_trace(trace)
    if b8 % 2 != 0:
        trace = generate_trace(filterdata, hiding_df, b8_col, 'Answer', False)
        fig.add_trace(trace)
    return fig


@app.callback(
    Output("locations_dd", "value"),
    Output("locations_dd", "options"),
    Input("map", "clickData"),
    Input('aggregate_graph', 'clickData'),
    Input('locations_dd', 'search_value'),
    State('locations_dd', 'options'),
    prevent_initial_call=True
)
def add_location_to_dropdown(click, agg_click, search, opts):
    """Alter the locations dropdown values and options."""
    if ctx.triggered_prop_ids == {'locations_dd.search_value': 'locations_dd'}:
        # print(ctx.triggered_prop_ids)
        with SQLiteConnection(db_name) as (conn, cursor):
            query = f"""
            SELECT DISTINCT KeywordLabel
            FROM KeywordsTable
            WHERE Latitude IS NOT NULL AND LOWER(KeywordLabel) LIKE LOWER("%{search}%") LIMIT 10
            ;"""
            cursor.execute(query)
            search = cursor.fetchall()
        return no_update, [r[0] for r in search] + opts
    if ctx.triggered_id == 'map':
        click_text = click["points"][0]["hovertext"]
        return click_text, [click_text]
    elif agg_click['points'][0]['customdata'][1] == 'CityOfBirth':
        return agg_click['points'][0]['label'], [agg_click['points'][0]['label']]
    else:
        return no_update


@app.callback(
    Output("list_of_people", "children"),
    Output('pagination', 'max_value'),
    Input('name-search-input', 'value'),
    Input('filter_store', 'data'),
    Input('pagination', 'active_page'),
    prevent_initial_call=True
)
def update_suggestions(search_value, stored_list, pagination):
    """Update the list of people and associated pagination."""
    batch_size = 14
    if not search_value or search_value == "":
        if stored_list:
            stored_list = unzip_store(stored_list)
            if pagination is None:
                pagination = 1
            selected_batch = get_batch(stored_list, batch_size, pagination)

            batch_list = tuple(selected_batch)
            if len(batch_list) == 0:
                list_statement = "1=1"
            elif len(batch_list) == 1:
                batch_list = f"({batch_list[0]}, {batch_list[0]})"
            list_statement = f"PIQPersonID IN {batch_list}"
        else:
            list_statement = "1=1"

        with SQLiteConnection(db_name) as (conn, cursor):
            query = f"""
            SELECT FullName, ImageURL, PIQPersonID, ExperienceGroup, DateOfBirth, CountryOfBirth
            FROM BioTable
            WHERE {list_statement}
            ;"""
            cursor.execute(query)
            results = cursor.fetchall()

        if not results:
            return html.Div(["Sorry, no people fit these criteria."]), math.ceil((len(stored_list) / batch_size))
        text_area = []
        for i in results:
            entry = dbc.Button([
                        dbc.Spinner(
                            dbc.CardImg(
                                src=i[1] if i[1] is not None else "assets/portrait-placeholder-wide.png",
                                alt=f"Portrait of survivor {i[0]}", style={'objectFit': 'contain'}
                                )),
                        dbc.Tooltip(
                            dcc.Markdown(f"**{i[0]}** ({i[3]})  \nBorn: {i[4]}  \n{i[5]}"),
                            target={'type': 'info_button', 'index': str(i[2])},
                        ),
                        ],
                        id={'type': 'info_button', 'index': str(i[2])},  # Unique ID for callback handling
                        n_clicks=0,
                        color="link",  # Makes button appear like a link (no background color)
                        style={
                            "flex": "1 1 auto",
                            "padding": 0,
                            # 'maxHeight': '20%',
                            'maxWidth': '50%'
                        }
                    )
            text_area.append(entry)
        return text_area, math.ceil((len(stored_list) / batch_size))

    if search_value:

        with SQLiteConnection(db_name) as (conn, cursor):
            query = """
                SELECT FullName, ImageURL, PIQPersonID, ExperienceGroup, DateOfBirth, CountryOfBirth
                FROM BioTable
                WHERE FullName LIKE ?
            """
            cursor.execute(query, (f'%{search_value}%',))
            results_pre = cursor.fetchall()

        results = get_batch(results_pre, batch_size, 1 if pagination is None else pagination)
        text_area = []
        for i in results:
            entry = dbc.Button([
                        dbc.Spinner(
                            dbc.CardImg(
                                src=i[1] if i[1] is not None else "assets/portrait-placeholder-wide.png",
                                alt="Portrait of survivor i[0]", style={'objectFit': 'contain'}
                                )),
                        dbc.Tooltip(
                            dcc.Markdown(f"**{i[0]}**  \n{i[3]}  \n{i[4]}  \n{i[5]}"),
                            target={'type': 'info_button', 'index': str(i[2])},
                        ),
                        ],
                        id={'type': 'info_button', 'index': str(i[2])},  # Unique ID for callback handling
                        n_clicks=0,
                        color="link",  # Makes button appear like a link (no background color)
                        style={
                            "flex": "1 1 auto",
                            "padding": 0,
                            # 'maxHeight': '20%',
                            'maxWidth': '50%'
                        }
                    )
            text_area.append(entry)
        if text_area == []:
            return ["I am sorry, we were unable to find somebody with that name."], 1
        return text_area, math.ceil((len(results_pre) / batch_size))
    return ["Click on the map to find people by associated area, or use the search bar for the entire dataset."], 1


@app.callback(
    Output("testimony_selectors", "children"),
    Input({'type': 'info_button', 'index': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def generate_testimonybuttons(clicked):
    """Generate buttons for the different personalized data, such as testimonies, maps, biographical info, etc."""
    if sum(clicked) > 0:
        piq = int(ctx.triggered_id["index"])
        with SQLiteConnection(db_name) as (conn, cursor):
            query = f"""
            SELECT IntCode, FullName
            FROM BioTable
            WHERE PIQPersonID = {piq}"""
            cursor.execute(query)
            result = cursor.fetchone()
            intcode = result[0]
            name = result[1]
            query = f"""
            SELECT TapeNumber
            FROM TestimonyTable
            WHERE IntCode = {intcode}"""
            cursor.execute(query)
            try:
                num_tapes = cursor.fetchall()[-1][0]
            except IndexError:
                num_tapes = None

        testimony_selectors = []
        entry = dbc.Button(f"About {name}", id={
                           'type': 'personinfo_button', 'intcode': intcode}, color='primary')
        testimony_selectors.append(entry)

        if num_tapes is None:
            return testimony_selectors

        for i in range(0, num_tapes):
            entry = dbc.Button(f"Testimony Part {i+1}",
                               id={'type': 'tape_button', 'index': i, 'intcode': intcode},
                               n_clicks=0, style={'marginLeft': '.2vw'})
            testimony_selectors.append(entry)
        return testimony_selectors
    else:
        return no_update


@app.callback(
    Output("testimony_area", "children"),
    Output('selected_piq_store', 'data'),
    Input({'type': 'tape_button', 'index': ALL, 'intcode': ALL}, "n_clicks"),
    Input({'type': 'personinfo_button', 'intcode': ALL}, "n_clicks"),
    prevent_initial_call=True
)
def retrieve_testimony(tape_btn, person_btn):
    """Fill the testimony area when the associated button is clicked."""
    if ctx.triggered_id.get('type') == 'personinfo_button':
        intcode = int(ctx.triggered_id['intcode'])
        with SQLiteConnection(db_name) as (conn, cursor):
            query = f"""
            SELECT
                PIQPersonID,
                FullName,
                ExperienceGroup,
                Gender,
                CityOfBirth,
                CountryOfBirth,
                DateOfBirth,
                InterviewDate,
                LanguageLabel,
                ImageURL,
                Aliases
            FROM BioTable
            WHERE IntCode = {intcode};"""
            cursor.execute(query)
            biodata = cursor.fetchone()

            query = f"""
                SELECT DISTINCT Answer, Latitude, Longitude
                FROM QuestionsTable Q
                LEFT JOIN KeywordsTable K ON Answer = KeywordLabel
                WHERE Q.intcode = {intcode} AND Latitude IS NOT NULL
            ;"""

            cursor.execute(query)
            locs = cursor.fetchall()

            cursor.execute(f"""SELECT CityOfBirth, Latitude, Longitude
                                        FROM BioTable LEFT JOIN KeywordsTable ON CityOfBirth = KeywordLabel
                                        WHERE BioTable.intcode = {intcode} AND Latitude IS NOT NULL""")
            birthplace = cursor.fetchone()
            if birthplace:
                locs.insert(0, birthplace)

            query = f"""
            SELECT Relationship, RelationName
            FROM PeopleTable
            WHERE IntCode = {intcode};"""
            peopledata_df = pd.read_sql_query(query, conn)
            peopledata_df = peopledata_df.groupby('Relationship').agg({
                'RelationName': '\n'.join
                }).reset_index()
            query = f"""
            SELECT QuestionText, Answer
            FROM QuestionsTable
            WHERE PIQPersonID = {biodata[0]};"""
            qa_df = pd.read_sql_query(query, conn)
            qa_df = qa_df.groupby('QuestionText').agg({
                'Answer': '\n'.join
                }).reset_index()

        if len(locs) > 0:
            city_names = [item[0] for item in locs]
            latitudes = [item[1] for item in locs]
            longitudes = [item[2] for item in locs]
            sizes = [20 for item in locs]
            symbols = ['star' if item[0] == biodata[4]
                       else 'castle' if item[0] in ghettos_df['KeywordLabel'].values
                       else 'danger' if item[0] in deathcamps_df['KeywordLabel'].values
                       else 'castle' if item[0] in allcamps_df['KeywordLabel'].values
                       else 'marker' for item in locs]

            fig = go.Figure(go.Scattermap(
                mode="markers",
                lon=longitudes, lat=latitudes,
                marker={'size': sizes, 'symbol': symbols, 'color': 'black'},
                hovertext=city_names,
                hoverinfo="text",
                ),
                )

            fig.update_layout(
                map={
                    'style': "open-street-map",
                    'zoom': 1,
                    'center': {'lat': latitudes[0], 'lon': longitudes[0]}
                },
                showlegend=False,
                autosize=True,
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                height=None,
                width=None
            )
            subjecto_mapping = {name: idx for idx, name in enumerate(geojson_df['SUBJ'].unique())}
            geojson_df['subj_id'] = geojson_df['SUBJ'].map(subjecto_mapping)

            trace = go.Choroplethmap(
                geojson=geojson_data,
                locations=geojson_df['NAME'],  # Link to GeoJSON by NAME
                z=geojson_df['subj_id'],  # Numeric ID for coloring based on SUBJECTO
                featureidkey="properties.NAME",  # Link GeoJSON properties.NAME to DataFrame
                colorscale=px.colors.qualitative.Light24,
                hoverinfo='skip',
                text=geojson_df['SUBJ'],
                marker={'opacity': 0.5},
                marker_line_width=1.5,
                marker_line_color='black',
                showscale=False
            )
            fig.add_trace(trace)

        else:
            fig = go.Figure(go.Scattermap())

        table = dash_table.DataTable(qa_df.to_dict('records'),
                                     [{"name": i, "id": i}
                                         for i in qa_df.columns],
                                     cell_selectable=False,
                                     style_cell={'textAlign': 'left', 'fontSize': '1.5vh',
                                                 'backgroundColor': color_scheme,
                                                 'whiteSpace': 'pre-line',
                                                 # 'wordBreak': 'break-all',
                                                 'overflowWrap': 'break-word',
                                                 },
                                     style_table={
            # 'width':'100%',
            'fontSize': '1vh'
        },
            style_header={'backgroundColor': 'black',
                          'fontWeight': 'bold',
                          'color': 'white'
                          },
            style_data_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)',
            }]
        )

        peopletable = dash_table.DataTable(peopledata_df.to_dict('records'),
                                           [{"name": i, "id": i}
                                               for i in peopledata_df.columns],
                                           cell_selectable=False,
                                           style_cell={'textAlign': 'left', 'fontSize': '1.5vh',
                                                       'backgroundColor': color_scheme,
                                                       'whiteSpace': 'pre-line',
                                                       # 'wordBreak': 'break-all',
                                                       'overflowWrap': 'break-word',
                                                       },
                                           style_table={
            # 'width':'100%',
            'fontSize': '1vh'
        },
            style_header={'backgroundColor': 'black',
                          'fontWeight': 'bold',
                          'color': 'white'
                          },
            style_data_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)',
            }]
        )

        added_col = [
            "Person ID:",
            "Full name:",
            "Experience:",
            "Gender:",
            "Place of Birth:",
            'Country of birth:',
            "Date of birth:",
            "Interview date:",
            "Interview language:",
            "Image URL:",
            "Aliases:"
        ]

        biodata_df = pd.DataFrame(({
            'Biographical': added_col,
            'Info': biodata
        }))
        biodata_df.iloc[-1, 1] = biodata_df.iloc[
            -1, 1
            ].replace(',', '\n').replace('[', '').replace(']', '').replace("'", "")
        info = dash_table.DataTable(biodata_df.to_dict('records'),
                                    cell_selectable=False,
                                    style_cell={'textAlign': 'left',
                                                'backgroundColor': color_scheme,
                                                'whiteSpace': 'pre-line',
                                                # 'wordBreak': 'break-all',
                                                'overflowWrap': 'break-word',
                                                },
                                    style_table={
                                         'fontSize': '1.5vh'
                                    },
                                    style_header={'backgroundColor': 'black',
                                                  'fontWeight': 'bold',
                                                  'color': 'white'
                                                  },
                                    style_data_conditional=[{
                                        'if': {'row_index': 'odd'},
                                        'backgroundColor': 'rgb(220, 220, 220)',
                                    }],
                                    style_as_list_view=True,
                                    )
        rows = [dbc.Button(item[0],
                           id={'type': 'map_place_button', 'name': item[0], 'lat': item[1], 'lon': item[2]},
                           n_clicks=0,
                           style={'marginLeft': '.5vw', 'color': 'white'},
                           className='btn btn-outline-primary'
                           ) for item in locs]
        combined_info = html.Div([
            dbc.Accordion([
                dbc.AccordionItem(
                    dbc.Spinner(
                        dbc.Col(
                            width={'width': 8, 'offset': 2}, style={'minHeight': '30vh'}, id='wc_area'
                        )),
                    title="WordCloud", style=accordion_style, item_id='wc_accordion'),
                dbc.AccordionItem([
                    dcc.Graph(figure=fig, id='personal_map', style={'height': '60vh'}, responsive=True),
                    *rows,
                    html.P("""Map indicating interviewee places visited.
                           The order assumes a chronological narrative in the testimony.
                           It may not be accurate to the real chronology of events.""", id='place_text'),
                ], title="Map", style=accordion_style),
                dbc.AccordionItem([

                    dbc.Row([
                        dbc.Col([
                            html.Img(src=biodata[9] if biodata[9] is not None
                                     else "assets/portrait-placeholder-wide.png", alt=f'Image of {biodata[1]}',
                                     style={
                                     "width": "auto",
                                     'maxWidth': '100%',
                                     'display': 'block',
                                     'margin': 'auto',
                                     'paddingBottom': '2vh'}),
                            table,
                            ], width=5),
                        dbc.Col([
                            info,
                            html.Br(),
                            peopletable
                            ], width=7)
                        ])

                ], item_id='bio_info_pane', title="Biographical Data", style=accordion_style),
            ], active_item='bio_info_pane', id='accordion'),
        ],)

        return combined_info, intcode

    if ctx.triggered_id.get('type') == 'tape_button':
        intcode = int(ctx.triggered_id["intcode"])
        tape_num = int(ctx.triggered_id["index"])
        tape_num += 1

        with SQLiteConnection(db_name) as (conn, cursor):
            query = f"""
            SELECT TapeTestimony
            FROM TestimonyTable
            WHERE IntCode = {intcode} AND TapeNumber = {tape_num};"""
            cursor.execute(query)
            testimony = cursor.fetchone()[0]
            query = f"""
            SELECT FullName, LanguageLabel
            FROM BioTable
            WHERE IntCode = {intcode};"""
            cursor.execute(query)
            biodata = cursor.fetchone()
        testimony = testimony.replace('?', "?\n")
        return dbc.Accordion([
            dbc.AccordionItem([
                dcc.Markdown(
                    testimony,
                    id='testimony_text',
                    style={
                        'backgroundColor': 'white',
                        'color': 'black',
                        "fontSize": "2vh",
                        'height': '100%',
                        'padding': '4vh',
                        'borderStyle': 'inset',
                        'whiteSpace': 'pre-line'
                        }),
               ], title="Testimony", style=accordion_style
            ),
            dbc.AccordionItem(
                dbc.Spinner(
                    dbc.Col(
                        width={'width': 8, 'offset': 2}, style={'minHeight': '30vh'}, id='wc_area'
                    )),
                title="WordCloud", style=accordion_style, item_id='wc_accordion'),
        ], active_item='wc_accordion', id='accordion'), [intcode, tape_num]
    else:
        return "", ""


@app.callback(
    Output('personal_map', 'figure'),
    Output('place_text', 'children'),
    Input({'type': 'map_place_button', 'name': ALL, 'lat': ALL, 'lon': ALL}, "n_clicks"),
    State('personal_map', 'figure')
    )
def individual_map_button_click_actions(nclick, fig):
    """Update some text and zoom user to locations in the individual map on button click."""
    if sum(nclick) == 0:
        return no_update, no_update
    with SQLiteConnection(db_name) as (conn, cursor):
        cursor.execute(
            f"""
            SELECT COUNT(*) as count, QuestionText
            FROM (
                SELECT DISTINCT PIQPersonID, QuestionText
                FROM QuestionsTable
                WHERE Answer = "{ctx.triggered_id['name']}"
            ) AS distinct_pairs
            GROUP BY QuestionText
            ORDER BY count DESC
            ;""")
        results = cursor.fetchall()

    lat = ctx.triggered_id['lat']
    lon = ctx.triggered_id['lon']
    fig['layout']['map']['zoom'] = 8
    fig['layout']['map']['center']['lat'] = lat
    fig['layout']['map']['center']['lon'] = lon

    return fig, [dcc.Markdown(f'{r[1]}: {r[0]}') for r in results]


@app.callback(
    Output('wc_area', 'children'),
    Input('accordion', 'children'),
    State('selected_piq_store', 'data')
    )
def generate_wordcloud(trigger, data):
    """Make the word cloud either for tapes or full texts."""
    with SQLiteConnection(db_name) as (conn, cursor):
        if type(data) is int:
            query = f"""
            SELECT TapeTestimony
            FROM TestimonyTable_fts
            WHERE IntCode = {data} """
        else:
            query = f"""
            SELECT TapeTestimony
            FROM TestimonyTable
            WHERE IntCode = {data[0]} AND TapeNumber = {data[1]};"""
        cursor.execute(query)
        testimony = cursor.fetchone()[0]

    testimony = kw_model.extract_keywords(docs=testimony, top_n=100, stop_words='english')
    testimony = [list(tup) for tup in testimony]
    testimony = normalise(testimony, vmax=80, vmin=15)
    wordcloud = DashWordcloud(
                    id='wordcloud',
                    list=testimony,
                    width=1000,
                    height=600,
                    gridSize=8,
                    color="random-light",
                    backgroundColor=color_scheme_secondary,
                    shuffle=True,
                    rotateRatio=0,
                    # shrinkToFit=True,
                    # shape='star',
                    hover=True,
                ),
    return wordcloud


@app.callback(
    Output("question_dd", "options"),
    Output("question_dd", "value"),
    Input('filter_store', 'data'),
    State('question_dd', 'value'),
    prevent_initial_call=True
)
def generate_questions_options(cohort, selected):
    """Update the questionnaire values and options based on group selected by user."""
    if cohort is None:
        return [], []
    cohort = unzip_store(cohort)
    with SQLiteConnection(db_name) as (conn, cursor):
        query = f"""
        SELECT DISTINCT QuestionText
        FROM QuestionsTable
        WHERE PIQPersonID in {cohort}
        ;
        """
        cursor.execute(query)
        expgroup_result = cursor.fetchall()
    if len(expgroup_result) == 0:
        return [], []
    expgroup_result_list = [tup[0] for tup in expgroup_result]
    return expgroup_result_list, selected if selected in expgroup_result_list else expgroup_result_list[0]


@app.callback(
    Output('birth', 'style'),
    Input('birth', 'n_clicks'),
)
def update_button_1(b1):
    """Update button."""
    if b1 % 2 == 0:
        return default_b_style

    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b1_col}'
    return colored_b


@app.callback(
    Output('intern', 'style'),
    Input('intern', 'n_clicks'),
)
def update_button_2(b2):
    """Update button."""
    if b2 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b2_col}'
    return colored_b


@app.callback(
    Output('pow', 'style'),
    Input('pow', 'n_clicks'),
)
def update_button_3(b3):
    """Update button."""
    if b3 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b3_col}'
    return colored_b


@app.callback(
    Output('ghetto', 'style'),
    Input('ghetto', 'n_clicks'),
)
def update_button_4(b4):
    """Update button."""
    if b4 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b4_col}'
    return colored_b


@app.callback(
    Output('concen', 'style'),
    Input('concen', 'n_clicks'),
)
def update_button_5(b5):
    """Update button."""
    if b5 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b5_col}'
    return colored_b


@app.callback(
    Output('death', 'style'),
    Input('death', 'n_clicks'),
)
def update_button_6(b6):
    """Update button."""
    if b6 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b6_col}'
    return colored_b


@app.callback(
    Output('liber', 'style'),
    Input('liber', 'n_clicks'),
)
def update_button_7(b7):
    """Update button."""
    if b7 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b7_col}'
    return colored_b


@app.callback(
    Output('hiding', 'style'),
    Input('hiding', 'n_clicks'),
)
def update_button_8(b8):
    """Update button."""
    if b8 % 2 == 0:
        return default_b_style
    colored_b = default_b_style.copy()
    colored_b['backgroundColor'] = f'{b8_col}'
    return colored_b


@app.callback(
    Output("answer_dd", "options"),
    Output("answer_dd", "value"),
    Input("questionnaire_graph", "clickData"),
    State('question_dd', 'value'),
    State('answer_dd', 'value'),
    State('answer_dd', 'options'),
    Input('answer_dd', 'search_value')
)
def update_answerdd(click, question, exist_values, exist_opts, search):
    """Update answer dropdown."""
    if ctx.triggered_prop_ids == {'answer_dd.search_value': 'answer_dd'}:
        with SQLiteConnection(db_name) as (conn, cursor):
            cursor.execute(f"""
                           SELECT DISTINCT QuestionText, Answer
                           FROM QuestionsTable
                           WHERE QuestionText LIKE '%{search}%' COLLATE NOCASE
                               OR Answer LIKE '%{search}%' COLLATE NOCASE
                           LIMIT 10;
                           """)
            search = cursor.fetchall()
        return [f'{r[0]}: {r[1]}' for r in search] + exist_opts, no_update
    if click:
        if not exist_values:
            exist_values = []
        click_text = click['points'][0]['label']
        if f'{question}: {click_text}' in exist_values:
            return no_update, no_update
        exist_values.append(f'{question}: {click_text}')
        return exist_values, exist_values
    else:
        return no_update, no_update


@app.callback(
    Output("country_dd", "value"),
    Input("aggregate_graph", "clickData"),
    State("country_dd", 'value'),
    prevent_initial_call=True
)
def update_countrydd(click, existing_values):
    """Update country dropdown based on clicked data."""
    if click and click['points'][0]['customdata'][1] == 'CountryOfBirth':
        existing_values.append(click['points'][0]['label'])
        return list(set(existing_values))
    return no_update


@app.callback(
    Output("gender_dd", "value"),
    Input("aggregate_graph", "clickData"),
    prevent_initial_call=True
)
def update_genderdd(click):
    """Update gender dropdown based on clicked data."""
    if click and click['points'][0]['customdata'][1] == 'Gender':
        return click['points'][0]['label']
    return no_update


@app.callback(
    Output("experience_dd", "value"),
    Input("aggregate_graph", "clickData"),
    prevent_initial_call=True
)
def update_experiencedd(click):
    """Update experience dropdown based on clicked data."""
    if click and click['points'][0]['customdata'][1] == 'ExperienceGroup':
        return click['points'][0]['label']
    return no_update


@app.callback(
    Output("language_dd", "value"),
    Input("aggregate_graph", "clickData"),
    prevent_initial_call=True
)
def update_languagedd(click):
    """Update language dropdown based on clicked data."""
    if click and click['points'][0]['customdata'][1] == 'LanguageLabel':
        return [click['points'][0]['label']]
    return no_update


@app.callback(
    Output("offcanvas", "is_open"),
    Output('canvas_button', 'disabled'),
    Input({'type': 'info_button', 'index': ALL}, "n_clicks"),
    Input('canvas_button', 'n_clicks'),
    prevent_initial_call=True
)
def toggle_offcanvas(n1, n2):
    """Open the offcanvas."""
    if ctx.triggered_id == 'canvas_button':
        return True, False
    elif sum(n1) > 0:
        return True, False
    else:
        return False, True


@app.callback(
    Output('aggregate_graph', 'figure'),
    Input('aggregate_dd', 'value'),
    Input("filter_store", "data"),
    prevent_initial_call=True
)
def update_aggregate_graph(select, someinput):
    """Update the aggregate graph by user criteria."""
    if someinput is not None and select is not None:
        someinput = unzip_store(someinput)

        if select == 'DateOfBirth':
            with SQLiteConnection(db_name) as (conn, cursor):
                query = f"""
                    SELECT DateOfBirth, PIQPersonID
                    FROM BioTable
                    WHERE PIQPersonID IN {someinput}
                ;"""
                cursor.execute(query)
                dates = cursor.fetchall()

            parsed_dates = []
            for date_str in dates:
                try:
                    string = date_str[0].replace(',', " ")
                    parsed_date = parser.parse(string)
                except (ValueError, TypeError, AttributeError):
                    continue  # Skip unparseable dates
                parsed_dates.append((parsed_date, date_str[1]))
            date_df = pd.DataFrame(parsed_dates, columns=['DateOfBirth', 'PIQPersonID'])
            # date_df['DateOfBirth'] = pd.to_datetime(date_df['DateOfBirth'], errors='coerce')

            date_format = '%b %d, %Y'
            date_df = date_df[
                date_df['DateOfBirth'].apply(lambda x: pd.to_datetime(x, format=date_format, errors='coerce')).notna()
                ]

            cutoff_date = pd.to_datetime('1950-12-31')
            date_df = date_df[date_df['DateOfBirth'] <= cutoff_date]

            fig = px.histogram(date_df,
                               x='DateOfBirth',
                               nbins=min(int(len(date_df['DateOfBirth'])/5), len(range(1892, 1945))),
                               color_discrete_sequence=[color_scheme_secondary]
                               )
            fig.update_layout(

                paper_bgcolor=color_scheme,
                plot_bgcolor=color_scheme,
                autosize=True,
                margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
                xaxis_title="",
                yaxis_title="",
                dragmode='pan'

            )
            fig.update_traces(
                customdata=date_df[[select]].assign(column_name=select),
                hovertemplate='%{x}<br>%{y}<extra></extra>'
            )
            return fig

        with SQLiteConnection(db_name) as (conn, cursor):
            query = f"""
                    SELECT {select}, COUNT(*) AS count
                    FROM BioTable
                    WHERE PIQPersonID IN {someinput}
                    GROUP BY {select}
                    ORDER BY count DESC
                    ;"""
            queryfiltered_df = pd.read_sql_query(query, conn)
        total_sum = queryfiltered_df['count'].sum()
        queryfiltered_df['Percentage'] = (
            queryfiltered_df['count'] / total_sum) * 100
        if len(queryfiltered_df) > 20:
            queryfiltered_df = queryfiltered_df[0:50]

        colors = []
        for i in range(len(queryfiltered_df)):
            if i <= 8:
                colors.append(px.colors.qualitative.Set1[i])
            else:
                colors.append('#d9d9d9')

        fig = px.bar(
            queryfiltered_df,
            y='count',
            x=select,
            text=queryfiltered_df['Percentage'].map('{:.2f}%'.format),
            color=select,
            color_discrete_sequence=colors,
        )
        fig.update_layout(
            xaxis={'range': [-.5, min(queryfiltered_df[select].nunique(), 8)]},
            paper_bgcolor='#EDEDED',
            plot_bgcolor='rgba(0,0,0,0)',
            autosize=True,
            margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
            xaxis_title="",
            yaxis_title="",
            dragmode='pan',
            showlegend=False,

        )
        fig.update_traces(
            customdata=queryfiltered_df[[select]].assign(column_name=select),
            hovertemplate='%{y}<br>%{text}<extra></extra>'
        )

        return fig
    return no_update


@app.callback(
    Output('counter', 'children'),
    Input('filter_store', 'data'),
    prevent_initial_call=True
    )
def generate_data_counter_graph(piq_list):
    """Generate table counting data displayed."""
    piq_list = unzip_store(piq_list)
    length = len(piq_list)
    if length == 2 and piq_list[1] == "":
        length = 1

    with SQLiteConnection(db_name) as (conn, cursor):
        query = """
            SELECT COUNT(DISTINCT PIQPersonID) as count
            FROM BioTable
        ;"""
        cursor.execute(query,)
        total = cursor.fetchone()[0]
    percentage = (length / total) * 100
    percentage = max(percentage, 0.01) if length > 0 else 0.00
    rows = [
            html.Tr([
                html.Td("SEARCH RESULTS",
                        style={'textAlign': 'left',
                               'backgroundColor': color_scheme,
                               'borderRight': '1px solid black'}
                        ),
                html.Td(f"{length} ({percentage:.2f}%)",
                        style={'textAlign': 'right',
                               'backgroundColor': color_scheme,
                               'borderRight': '1px solid black'}),
            ])
    ]
    return rows


@app.callback(
    Output('testimony_dd', 'options'),
    Output('testimony_dd', 'value'),
    Input('search_in_testimony_input', 'value'),
    Input('wordcloud', 'click'),
    State('testimony_dd', 'options'),
    State('testimony_dd', 'value'),
    prevent_initial_call=True
    )
def fill_testimony_dd(user_input, click, options, vals):
    """Use to fill out the dropdown based on user input."""
    if ctx.triggered_id == 'wordcloud' and click:
        if vals is None:
            vals = []
        options.append(click[0])
        vals.append(click[0])
        return options, vals
    if user_input:
        if vals is None:
            vals = []
        options.append(user_input)
        vals.append(user_input)
        return options, vals
    else:
        return no_update, no_update


@app.callback(
    Output('tab1', 'children'),
    Input('filter_store', 'data')
    )
def update_keywordcloud(filterdata):
    """Update the keyword cloud."""
    filterdata = unzip_store(filterdata)
    with SQLiteConnection(db_name) as (conn, cursor):
        query = f"""
        SELECT KeywordLabel, COUNT(*) as count
        FROM KeywordsTable
        WHERE PIQPersonID IN {filterdata} AND Latitude IS NULL AND KeywordLabel NOT LIKE '%(stills)'
        GROUP BY KeywordLabel
        ORDER BY count DESC
        --LIMIT 1000
        ;"""
        cursor.execute(query)
        keywords = cursor.fetchall()
    keywords = [[k[0], k[1]] for k in keywords]
    keywords = normalise(keywords)
    wordcloud = DashWordcloud(
                    id='keyword_wc',
                    list=keywords,
                    width=400,
                    height=250,
                    gridSize=4,
                    color="random-light",
                    backgroundColor='black',
                    shuffle=True,
                    rotateRatio=0.0,
                    # drawOutOfBound=False,
                    # shrinkToFit=True,
                    # shape='square',
                    hover=True,
                ),
    return wordcloud


@app.callback(
    Output('datatable', 'data'),
    Input('filter_store', 'data')
    )
def update_keyword_table(filterdata):
    """Update keyword table."""
    filterdata = unzip_store(filterdata)
    with SQLiteConnection(db_name) as (conn, cursor):
        query = f"""
        SELECT KeywordLabel, COUNT(*) as count, ParentLabel, RootLabel
        FROM KeywordsTable
        WHERE PIQPersonID IN {filterdata} AND Latitude IS NULL AND KeywordLabel NOT LIKE '%(stills)'
        GROUP BY KeywordLabel
        ORDER BY count DESC
        ;
        """
        df = pd.read_sql_query(query, conn)
    return df.to_dict('records')


@app.callback(
    Output('keyword_dd', 'value'),
    Output('keyword_dd', 'options'),
    Input('datatable', 'active_cell'),
    Input('keyword_wc', 'click'),
    State('datatable', 'derived_viewport_data'),
    State('keyword_dd', 'value'),
    State('keyword_dd', 'options'),
    Input('keyword_dd', 'search_value'),
    prevent_initial_call=True
    )
def update_keyworddd(click, wc_click, data, ex_val, ex_opt, search):
    """Update the keyword dropdown menu. Dynamically generates options so as to not overload DOM."""
    if ctx.triggered_prop_ids == {'keyword_dd.search_value': 'keyword_dd'}:
        with SQLiteConnection(db_name) as (conn, cursor):
            cursor.execute(f"""SELECT KeywordID, KeywordLabel
                           FROM KeywordsTable WHERE
                           KeywordLabel LIKE '%{search}%' COLLATE NOCASE LIMIT 10 """)
            search = cursor.fetchall()

        return no_update, [{'label': r[1], 'value': r[0]} for r in search] + ex_opt
    if click and click.get('column_id') in ['count', 'ParentLabel', 'RootLabel', None]:
        return no_update, no_update
    if click or wc_click:
        if not ex_opt:
            ex_opt = []
        if ctx.triggered_id == 'datatable':
            col = click['column_id']
            row = click['row']
            t = data[row][col]
            with SQLiteConnection(db_name) as (conn, cursor):
                cursor.execute(f"""SELECT KeywordID FROM KeywordsTable WHERE KeywordLabel = "{t}" """)
                result = cursor.fetchone()[0]
            if result in ex_val:
                return no_update, no_update
            ex_val.append(result)
            ex_opt.append({'label': t, 'value': result})

            return ex_val, ex_opt
        elif ctx.triggered_id == 'keyword_wc':
            with SQLiteConnection(db_name) as (conn, cursor):
                cursor.execute(f"""SELECT KeywordID FROM KeywordsTable WHERE KeywordLabel = "{wc_click[0]}" """)
                result = cursor.fetchone()[0]
            if result in ex_val:
                return no_update, no_update
            ex_val.append(result)
            ex_opt.append({'label': wc_click[0], 'value': result})

            return ex_val, ex_opt
    return no_update, no_update


@app.callback(
    Output("place_info", "children"),
    Output('place_info', 'duration'),
    Output('place_info', 'header'),
    Output('place_info', 'is_open'),
    Input("map", "clickData"),  # Map click as an input
    prevent_initial_call=True
)
def generate_annotation(click):
    """Create an annotation for the map when an area is clicked on."""
    click_text = click["points"][0]["hovertext"]
    annotations = []

    query1 = """SELECT COUNT(DISTINCT PIQPersonID) as count FROM BioTable WHERE CityOfBirth = ?"""
    query2 = """
        SELECT COUNT(*) as count, QuestionText
        FROM (
            SELECT DISTINCT PIQPersonID, QuestionText
            FROM QuestionsTable
            WHERE Answer = ?
        ) AS distinct_pairs
        GROUP BY QuestionText
        ORDER BY count DESC
        ;"""
    with SQLiteConnection(db_name) as (conn, cursor):
        cursor.execute(query1, (click_text,))
        n_births = cursor.fetchone()[0]
        question_count_df = pd.read_sql_query(query2, conn, params=(click_text,))

    if n_births != 0:
        annotations.append(html.P(f'As Birthplace: {n_births}', style={
                           "margin": "0", "fontSize": "1vh"}))
    for _, row in question_count_df.iterrows():
        annotations.append(html.P(f"{row['QuestionText']}: {row['count']}", style={
                           "margin": "0", "fontSize": "1vh"}))

    return annotations, 10000, click_text, True


@app.callback(
    Output('range_slider_values', 'children'),
    Input('yearborn_dd', 'value')
    )
def show_rangeslider_values(values):
    return f"Born: {values[0]} - {values[1]}"


@app.callback(
    Output('search_in_testimony_input', 'value'),
    Input('search_in_testimony_input', 'n_blur')
    )
def clear_searchbar(value):
    return ""


if __name__ == "__main__":
    app.run_server(debug=False, use_reloader=False)
    # app.run_server(debug=False, use_reloader=False, threaded=False, host='your ip or something', port=8050)
