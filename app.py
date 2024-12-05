from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://*****/music')

artists = pd.read_sql(r'select ID value, Name label from music.Artist', engine)

app = Dash()

app.layout = [
    html.H1(children='Billboard Hot 100 Artist 2024', style={'textAlign':'center'}),
    dcc.Dropdown(artists.to_dict('records'), artists.iloc[0]['value'], id='dropdown-selection'),
    html.Img(alt='artist', style={'width': '200px'}, id='artist-image'),
    dcc.Graph(id='spotify-content'),
    dcc.Graph(id='spotify-change'),
    dcc.Graph(id='billboard-content')
]

@callback(
    Output('spotify-content', 'figure'),
    Output('billboard-content', 'figure'),
    Output('artist-image', 'src'),
    Output('spotify-change', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):

    df = pd.read_sql(f"""
        select l.ArtistId, a.Name Artist, a.ImageURL, max(l.SpotifyFollowers) SpotifyFollowers, date(l.UpdateTime) UpdateTime
        from music.ArtistLog l
        join music.Artist a on l.ArtistId=a.Id
        where l.ArtistId = {value}
        group by l.ArtistId, date(l.UpdateTime)"""
        , engine)
    fig = px.line(df, x='UpdateTime', y='SpotifyFollowers')
    fig.update_xaxes(type='category')
    img_url = df.iloc[0]['ImageURL']

    df = pd.read_sql("""
    with follower_series as (
                select l.ArtistId, a.Name Artist, a.ImageURL, max(l.SpotifyFollowers) SpotifyFollowers, date(l.UpdateTime) UpdateTime
                from music.ArtistLog l
                join music.Artist a on l.ArtistId=a.Id
                where l.ArtistId = 218
                group by l.ArtistId, date(l.UpdateTime)
        )
        select x.UpdateTime, (x.SpotifyFollowers - max(y.SpotifyFollowers)) ChangeFollowers
        from (
                select cur.UpdateTime, max(cur.SpotifyFollowers) SpotifyFollowers, max(pre.UpdateTime) PrevUpdateTime
                from follower_series cur
                join follower_series pre on cur.UpdateTime > pre.UpdateTime
                group by cur.UpdateTime
        ) x
        join follower_series y on x.PrevUpdateTime = y.UpdateTime
        group by x.UpdateTime""", engine)

    fig2 = px.line(df, x='UpdateTime', y='ChangeFollowers')
    fig2.update_xaxes(type='category')

    df = pd.read_sql(f"""
        select ca.ArtistId, ca.Position ChartPosition, c.ChartDate
        from music.ChartArtist ca
        join music.BillboardChart c
            on ca.ChartId = c.Id
        where ca.ArtistId = {value}
    """
        , engine)
    fig3 = px.scatter(df, x='ChartDate', y='ChartPosition')
    fig3.update_xaxes(type='category')
    fig3.update_yaxes(autorange="reversed", range=[1, 100])

    return fig, fig2, img_url, fig3

if __name__ == '__main__':
    app.run_server(host='172.31.42.9', port=8050, debug=True)
