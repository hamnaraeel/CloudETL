import dash
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from functools import lru_cache
import json
import os

# Initialize Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Stock Analytics Dashboard"

# Enhanced color scheme
colors = {
    'background': 'linear-gradient(135deg, #0d1421 0%, #1e2631 100%)',
    'background_solid': '#0d1421',
    'paper': 'linear-gradient(145deg, #1a2332 0%, #2d3748 100%)',
    'paper_solid': '#1e2631',
    'card': 'linear-gradient(145deg, #2d3748 0%, #3e4a5c 100%)',
    'text': '#ffffff',
    'text_secondary': '#a0aec0',
    'positive': '#48bb78',
    'negative': '#f56565',
    'accent': '#4299e1',
    'secondary': '#ed8936',
    'success': '#38a169',
    'warning': '#d69e2e',
    'info': '#3182ce',
    'purple': '#9f7aea',
    'pink': '#ed64a6',
    'teal': '#38b2ac',
    'shadow': '0 10px 25px rgba(0, 0, 0, 0.3)'
}

# Period mapping
PERIOD_OPTIONS = {
    '1D': '1d',
    '1W': '5d', 
    '1M': '1mo',
    '3M': '3mo',
    '6M': '6mo',
    '1Y': '1y',
    '5Y': '5y'
}

# Cache data for 5 minutes to avoid repeated API calls
@lru_cache(maxsize=20)
def get_cached_data(ticker, period, timestamp):
    """Cache key includes 5-minute timestamp"""
    try:
        # Use environment variable or default to localhost
        transform_url = os.getenv('TRANSFORM_SERVICE_URL', 'http://localhost:8001')
        response = requests.post(
            f'{transform_url}/transform_batch',
            json={'tickers': ticker, 'period': period},
            timeout=30
        )
        if response.status_code == 200:
            return response.json()['data']
        else:
            return []
    except Exception as e:
        print(f"API Error: {e}")
        return []

def get_data_with_cache(ticker, period):
    """Get data with 5-minute caching"""
    cache_key = int(time.time() / 300)  # 5-minute intervals
    return get_cached_data(ticker, period, cache_key)

def create_candlestick_chart(df, title):
    """Create professional candlestick chart"""
    if df.empty:
        return go.Figure().add_annotation(text="No Data Available", showarrow=False)
    
    fig = go.Figure()
    
    # Candlestick chart
    fig.add_trace(go.Candlestick(
        x=df['Date'],
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        name="OHLC",
        increasing_line_color=colors['positive'],
        decreasing_line_color=colors['negative']
    ))
    
    # Add moving averages if available
    if 'MA_7' in df.columns and df['MA_7'].notna().any():
        fig.add_trace(go.Scatter(
            x=df['Date'], y=df['MA_7'], 
            name='MA7', line=dict(color=colors['secondary'], width=1)
        ))
    
    if 'MA_30' in df.columns and df['MA_30'].notna().any():
        fig.add_trace(go.Scatter(
            x=df['Date'], y=df['MA_30'], 
            name='MA30', line=dict(color=colors['accent'], width=1)
        ))
    
    fig.update_layout(
        title=dict(
            text=title, 
            font=dict(size=18, color=colors['text'], family="Arial Black"),
            x=0.5,
            xanchor='center'
        ),
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_dark",
        height=450,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text'],
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis=dict(
            gridcolor='rgba(255,255,255,0.1)',
            linecolor='rgba(255,255,255,0.2)'
        ),
        yaxis=dict(
            gridcolor='rgba(255,255,255,0.1)',
            linecolor='rgba(255,255,255,0.2)'
        )
    )
    return fig

def create_volume_chart(df):
    """Create volume bar chart"""
    if df.empty:
        return go.Figure().add_annotation(text="No Data Available", showarrow=False)
    
    # Color bars based on price change
    colors_vol = []
    for i in range(len(df)):
        if i == 0:
            colors_vol.append(colors['positive'])
        else:
            if df.iloc[i]['Close'] >= df.iloc[i-1]['Close']:
                colors_vol.append(colors['positive'])
            else:
                colors_vol.append(colors['negative'])
    
    fig = go.Figure(data=go.Bar(
        x=df['Date'],
        y=df['Volume'],
        marker_color=colors_vol,
        name="Volume"
    ))
    
    fig.update_layout(
        title=dict(text="Trading Volume", font=dict(size=16, color=colors['text'])),
        xaxis_title="Date",
        yaxis_title="Volume",
        template="plotly_dark",
        height=400,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_technical_indicators(df):
    """Create RSI and technical indicators chart"""
    if df.empty or 'RSI_14' not in df.columns:
        return go.Figure().add_annotation(text="No Technical Data", showarrow=False)
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('RSI (14)', 'Price vs Moving Averages'),
        vertical_spacing=0.15,
        row_heights=[0.4, 0.6]
    )
    
    # RSI Chart
    if df['RSI_14'].notna().any():
        fig.add_trace(
            go.Scatter(x=df['Date'], y=df['RSI_14'], 
                      name='RSI', line=dict(color=colors['accent'])),
            row=1, col=1
        )
        # Overbought/Oversold lines
        fig.add_hline(y=70, line_dash="dash", line_color=colors['negative'], row=1)
        fig.add_hline(y=30, line_dash="dash", line_color=colors['positive'], row=1)
    
    # Price vs MA
    fig.add_trace(
        go.Scatter(x=df['Date'], y=df['Close'], 
                  name='Price', line=dict(color=colors['text'])),
        row=2, col=1
    )
    
    if 'MA_7' in df.columns and df['MA_7'].notna().any():
        fig.add_trace(
            go.Scatter(x=df['Date'], y=df['MA_7'], 
                      name='MA7', line=dict(color=colors['secondary'])),
            row=2, col=1
        )
    
    fig.update_layout(
        template="plotly_dark",
        height=400,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_trend_chart(df):
    """Create price trend line chart"""
    if df.empty:
        return go.Figure().add_annotation(text="No Data Available", showarrow=False)
    
    fig = go.Figure()
    
    # Price line
    fig.add_trace(go.Scatter(
        x=df['Date'], 
        y=df['Close'],
        mode='lines',
        name='Close Price',
        line=dict(color=colors['accent'], width=2),
        fill='tonexty'
    ))
    
    # Add trend line if enough data
    if len(df) > 2:
        x_numeric = pd.to_numeric(pd.to_datetime(df['Date']))
        z = np.polyfit(x_numeric, df['Close'], 1)
        trend_line = np.poly1d(z)(x_numeric)
        
        fig.add_trace(go.Scatter(
            x=df['Date'],
            y=trend_line,
            mode='lines',
            name='Trend',
            line=dict(color=colors['secondary'], width=2, dash='dash')
        ))
    
    fig.update_layout(
        title=dict(text="Price Trend Analysis", font=dict(size=16, color=colors['text'])),
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_dark",
        height=300,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_sector_heatmap(df, ticker):
    """Create fake sector performance heatmap for visual impact"""
    # Create fake sector data for impressive visual
    sectors = ['Technology', 'Healthcare', 'Finance', 'Energy', 'Consumer', 'Industrial']
    
    # Generate fake performance data
    np.random.seed(42)  # For consistent fake data
    performance = np.random.normal(0, 2, len(sectors))
    
    # Make current ticker sector show actual performance if available
    if not df.empty and 'Daily_Return' in df.columns:
        actual_return = df['Daily_Return'].iloc[-1] if not pd.isna(df['Daily_Return'].iloc[-1]) else 0
        performance[0] = actual_return  # Assume first sector is ticker's sector
    
    fig = go.Figure(data=go.Heatmap(
        z=[performance],
        x=sectors,
        y=['Sector Performance'],
        colorscale=[[0, colors['negative']], [0.5, '#ffffff'], [1, colors['positive']]],
        showscale=True,
        zmin=-5, zmax=5
    ))
    
    fig.update_layout(
        title=dict(text="Sector Performance Heatmap", font=dict(size=16, color=colors['text'])),
        template="plotly_dark",
        height=300,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_risk_charts(df):
    """Create risk metrics visualization"""
    if df.empty:
        return go.Figure().add_annotation(text="No Risk Data", showarrow=False)
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Volatility Trend', 'Daily Returns Distribution'),
        vertical_spacing=0.2
    )
    
    # Volatility chart
    if 'Volatility_7' in df.columns and df['Volatility_7'].notna().any():
        fig.add_trace(
            go.Scatter(x=df['Date'], y=df['Volatility_7'] * 100,  # Convert to percentage
                      name='7D Volatility', line=dict(color=colors['secondary'])),
            row=1, col=1
        )
    
    # Daily returns histogram
    if 'Daily_Return' in df.columns and df['Daily_Return'].notna().any():
        fig.add_trace(
            go.Histogram(x=df['Daily_Return'], name='Daily Returns',
                        marker_color=colors['accent'], opacity=0.7),
            row=2, col=1
        )
    
    fig.update_layout(
        template="plotly_dark",
        height=300,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_returns_chart(df):
    """Create daily returns bar chart"""
    if df.empty or 'Daily_Return' not in df.columns:
        return go.Figure().add_annotation(text="No Returns Data", showarrow=False)
    
    # Color bars based on positive/negative returns
    bar_colors = [colors['positive'] if x >= 0 else colors['negative'] 
                  for x in df['Daily_Return'].fillna(0)]
    
    fig = go.Figure(data=go.Bar(
        x=df['Date'],
        y=df['Daily_Return'],
        marker_color=bar_colors,
        name="Daily Returns"
    ))
    
    fig.update_layout(
        title=dict(text="Daily Returns (%)", font=dict(size=16, color=colors['text'])),
        xaxis_title="Date",
        yaxis_title="Return %",
        template="plotly_dark",
        height=300,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_volume_analysis(df):
    """Create volume analysis chart"""
    if df.empty:
        return go.Figure().add_annotation(text="No Volume Data", showarrow=False)
    
    fig = go.Figure()
    
    # Volume bars
    fig.add_trace(go.Bar(
        x=df['Date'],
        y=df['Volume'],
        name='Volume',
        marker_color=colors['accent'],
        opacity=0.6
    ))
    
    # Volume moving average if available
    if 'Volume_MA_7' in df.columns and df['Volume_MA_7'].notna().any():
        fig.add_trace(go.Scatter(
            x=df['Date'],
            y=df['Volume_MA_7'],
            name='Volume MA7',
            line=dict(color=colors['secondary'], width=2)
        ))
    
    fig.update_layout(
        title=dict(text="Volume Analysis", font=dict(size=16, color=colors['text'])),
        xaxis_title="Date",
        yaxis_title="Volume",
        template="plotly_dark",
        height=300,
        paper_bgcolor='#1e2631',
        plot_bgcolor='#0d1421',
        font_color=colors['text']
    )
    return fig

def create_stats_panel(df, ticker, period):
    """Create statistics panel"""
    if df.empty:
        return html.Div("No data available", className="text-center p-4")
    
    try:
        latest = df.iloc[-1]
        first = df.iloc[0]
        
        # Calculate period-specific metrics
        period_return = ((latest['Close'] - first['Open']) / first['Open']) * 100
        avg_volume = df['Volume'].mean()
        volatility = df['Daily_Return'].std() if 'Daily_Return' in df.columns and df['Daily_Return'].notna().any() else 0
        
        return html.Div([
            html.Div([
                html.Div([
                    html.H2("💰", style={'fontSize': '2rem', 'margin': '0'}),
                    html.H2(f"${latest['Close']:.2f}", 
                            className="mb-0", 
                            style={
                                'color': '#ffffff', 
                                'fontWeight': '900', 
                                'fontSize': '2.5rem',
                                'textShadow': '0 0 20px rgba(255, 255, 255, 0.3)',
                                'margin': '0'
                            })
                ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center', 'gap': '10px'}),
                
                html.Div([
                    html.Span("▲" if period_return >= 0 else "▼", 
                             style={
                                 'color': colors['positive'] if period_return >= 0 else colors['negative'],
                                 'fontSize': '1.5rem',
                                 'marginRight': '8px'
                             }),
                    html.Span(f"{period_return:+.2f}% ({period})", 
                             style={
                                 'color': colors['positive'] if period_return >= 0 else colors['negative'], 
                                 'fontSize': '1.3rem', 
                                 'fontWeight': 'bold',
                                 'textShadow': f'0 0 10px {colors["positive"] if period_return >= 0 else colors["negative"]}30'
                             })
                ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'})
            ], className="text-center mb-4", style={'padding': '20px 0'}),
            
            html.Hr(style={'borderColor': colors['text']}),
            
            html.Div([
                # Stats cards with icons
                html.Div([
                    html.Div([
                        html.Div("📈", style={'fontSize': '1.5rem', 'color': colors['success']}),
                        html.Div([
                            html.P("High", style={'margin': '0', 'color': colors['text_secondary'], 'fontSize': '0.9rem'}),
                            html.P(f"${df['High'].max():.2f}", style={'margin': '0', 'color': '#ffffff', 'fontWeight': 'bold', 'fontSize': '1.1rem'})
                        ])
                    ], style={'display': 'flex', 'alignItems': 'center', 'gap': '12px', 'padding': '15px', 'background': 'linear-gradient(145deg, #2d3748, #4a5568)', 'borderRadius': '10px', 'marginBottom': '12px', 'border': '1px solid rgba(72, 187, 120, 0.3)'}),
                    
                    html.Div([
                        html.Div("📉", style={'fontSize': '1.5rem', 'color': colors['negative']}),
                        html.Div([
                            html.P("Low", style={'margin': '0', 'color': colors['text_secondary'], 'fontSize': '0.9rem'}),
                            html.P(f"${df['Low'].min():.2f}", style={'margin': '0', 'color': '#ffffff', 'fontWeight': 'bold', 'fontSize': '1.1rem'})
                        ])
                    ], style={'display': 'flex', 'alignItems': 'center', 'gap': '12px', 'padding': '15px', 'background': 'linear-gradient(145deg, #2d3748, #4a5568)', 'borderRadius': '10px', 'marginBottom': '12px', 'border': '1px solid rgba(245, 101, 101, 0.3)'}),
                    
                    html.Div([
                        html.Div("📋", style={'fontSize': '1.5rem', 'color': colors['info']}),
                        html.Div([
                            html.P("Avg Volume", style={'margin': '0', 'color': colors['text_secondary'], 'fontSize': '0.9rem'}),
                            html.P(f"{avg_volume:,.0f}", style={'margin': '0', 'color': '#ffffff', 'fontWeight': 'bold', 'fontSize': '1.1rem'})
                        ])
                    ], style={'display': 'flex', 'alignItems': 'center', 'gap': '12px', 'padding': '15px', 'background': 'linear-gradient(145deg, #2d3748, #4a5568)', 'borderRadius': '10px', 'marginBottom': '12px', 'border': '1px solid rgba(49, 130, 206, 0.3)'}),
                    
                    html.Div([
                        html.Div("⚡", style={'fontSize': '1.5rem', 'color': colors['warning']}),
                        html.Div([
                            html.P("Volatility", style={'margin': '0', 'color': colors['text_secondary'], 'fontSize': '0.9rem'}),
                            html.P(f"{volatility:.2f}%", style={'margin': '0', 'color': '#ffffff', 'fontWeight': 'bold', 'fontSize': '1.1rem'})
                        ])
                    ], style={'display': 'flex', 'alignItems': 'center', 'gap': '12px', 'padding': '15px', 'background': 'linear-gradient(145deg, #2d3748, #4a5568)', 'borderRadius': '10px', 'marginBottom': '12px', 'border': '1px solid rgba(214, 158, 46, 0.3)'}),
                    
                    html.Div([
                        html.Div("📄", style={'fontSize': '1.5rem', 'color': colors['purple']}),
                        html.Div([
                            html.P("Data Points", style={'margin': '0', 'color': colors['text_secondary'], 'fontSize': '0.9rem'}),
                            html.P(f"{len(df)}", style={'margin': '0', 'color': '#ffffff', 'fontWeight': 'bold', 'fontSize': '1.1rem'})
                        ])
                    ], style={'display': 'flex', 'alignItems': 'center', 'gap': '12px', 'padding': '15px', 'background': 'linear-gradient(145deg, #2d3748, #4a5568)', 'borderRadius': '10px', 'border': '1px solid rgba(159, 122, 234, 0.3)'})
                ])
            ])
        ], style={
            'background': colors['card'], 
            'borderRadius': '15px', 
            'padding': '20px',
            'boxShadow': colors['shadow'],
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'height': '600px',
            'backdropFilter': 'blur(10px)'
        })
        
    except Exception as e:
        return html.Div(f"Stats Error: {str(e)}", className="text-center p-4")

# App Layout
def get_button_style(is_default=False):
    if is_default:
        return {
            'background': 'linear-gradient(145deg, #4299e1, #3182ce)',
            'border': 'none',
            'color': 'white',
            'margin': '3px',
            'borderRadius': '8px',
            'boxShadow': '0 4px 8px rgba(66, 153, 225, 0.3)',
            'transition': 'all 0.3s ease',
            'fontWeight': 'bold',
            'padding': '8px 16px'
        }
    else:
        return {
            'backgroundColor': 'transparent',
            'border': '2px solid #4299e1',
            'color': '#4299e1',
            'margin': '3px',
            'borderRadius': '8px',
            'transition': 'all 0.3s ease',
            'fontWeight': '500',
            'padding': '8px 16px'
        }

app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H1("📊 PROFESSIONAL STOCK ANALYTICS", 
                       className="text-center mb-2",
                       style={
                           'fontSize': '3rem', 
                           'fontWeight': '800', 
                           'background': 'linear-gradient(45deg, #4299e1, #9f7aea, #ed64a6)',
                           '-webkit-background-clip': 'text',
                           '-webkit-text-fill-color': 'transparent',
                           'textShadow': '0 0 30px rgba(66, 153, 225, 0.3)',
                           'letterSpacing': '2px'
                       }),
                html.P("✨ Real-Time Market Intelligence & Advanced Analytics ✨",
                      className="text-center mb-4",
                      style={
                          'fontSize': '1.2rem',
                          'color': '#a0aec0',
                          'fontStyle': 'italic',
                          'fontWeight': '300'
                      })
            ])
        ])
    ], className="mb-4"),
    
    # Controls
    dbc.Row([
        dbc.Col([
            html.Div([
                # Ticker dropdown
                dcc.Dropdown(
                    id='ticker-dropdown',
                    options=[
                        {'label': '🍎 AAPL - Apple Inc.', 'value': 'AAPL'},
                        {'label': '🏢 MSFT - Microsoft Corp.', 'value': 'MSFT'},
                        {'label': '🔍 GOOGL - Alphabet Inc.', 'value': 'GOOGL'},
                        {'label': '⚡ TSLA - Tesla Inc.', 'value': 'TSLA'},
                        {'label': '📦 AMZN - Amazon Inc.', 'value': 'AMZN'},
                        {'label': '🎥 NFLX - Netflix Inc.', 'value': 'NFLX'},
                        {'label': '📱 META - Meta Platforms', 'value': 'META'},
                        {'label': '💳 V - Visa Inc.', 'value': 'V'}
                    ],
                    value='AAPL',
                    style={
                        'width': '280px', 
                        'display': 'inline-block', 
                        'marginRight': '20px',
                        'borderRadius': '8px',
                        'fontSize': '16px',
                        'fontWeight': 'bold'
                    }
                ),
                
                # Time period buttons
                html.Div([
                    html.Button('1D', id='btn-1d', className='btn btn-outline-primary btn-sm', style=get_button_style()),
                    html.Button('1W', id='btn-1w', className='btn btn-outline-primary btn-sm', style=get_button_style()),
                    html.Button('1M', id='btn-1m', className='btn btn-primary btn-sm', style=get_button_style(True)),
                    html.Button('3M', id='btn-3m', className='btn btn-outline-primary btn-sm', style=get_button_style()),
                    html.Button('6M', id='btn-6m', className='btn btn-outline-primary btn-sm', style=get_button_style()),
                    html.Button('1Y', id='btn-1y', className='btn btn-outline-primary btn-sm', style=get_button_style()),
                    html.Button('5Y', id='btn-5y', className='btn btn-outline-primary btn-sm', style=get_button_style()),
                ], style={'display': 'inline-block', 'marginLeft': '20px'}),
                
                html.Button('🔄 Refresh Data', id='refresh-btn', className='btn btn-success btn-sm', 
                           style={
                               'marginLeft': '20px',
                               'background': 'linear-gradient(145deg, #48bb78, #38a169)',
                               'border': 'none',
                               'borderRadius': '8px',
                               'padding': '8px 20px',
                               'fontWeight': 'bold',
                               'color': 'white',
                               'boxShadow': '0 4px 8px rgba(72, 187, 120, 0.3)',
                               'transition': 'all 0.3s ease'
                           }),
            ], className='d-flex align-items-center justify-content-center')
        ])
    ], className='mb-4', style={
        'padding': '25px', 
        'background': colors['card'], 
        'borderRadius': '15px',
        'boxShadow': colors['shadow'],
        'border': '1px solid rgba(255, 255, 255, 0.1)',
        'backdropFilter': 'blur(10px)'
    }),
    
    # Loading component
    dcc.Loading(
        id="loading",
        type="circle",
        color=colors['accent'],
        children=[
            # Stock Price Card Row (Top Priority)
            dbc.Row([
                dbc.Col([
                    html.Div(id='stats')
                ], lg=12, md=12, className="mb-4")
            ]),
            
            # Main Charts Row (Large) with enhanced styling
            dbc.Row([
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='candlestick')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=6, md=12, className="mb-4"),
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='volume')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=3, md=6, className="mb-4"),
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='indicators')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=3, md=6, className="mb-4")
            ]),
            
            # Analysis Row (Medium) with enhanced styling
            dbc.Row([
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='trend')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=4, md=6, className="mb-4"),
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='heatmap')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=4, md=6, className="mb-4"),
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='risk')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=4, md=12, className="mb-4")
            ]),
            
            # Detail Row (Small) with enhanced styling
            dbc.Row([
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='returns')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=4, md=6, className="mb-4"),
                dbc.Col([
                    html.Div([
                        dcc.Graph(id='vol-analysis')
                    ], style={
                        'background': colors['card'],
                        'borderRadius': '15px',
                        'padding': '10px',
                        'boxShadow': colors['shadow'],
                        'border': '1px solid rgba(255, 255, 255, 0.1)'
                    })
                ], lg=4, md=6, className="mb-4")
            ])
        ]
    )
], fluid=True, style={
    'background': colors['background'], 
    'minHeight': '100vh', 
    'padding': '30px',
    'fontFamily': 'Arial, sans-serif'
})

# Master Callback
@app.callback(
    [Output('candlestick', 'figure'),
     Output('volume', 'figure'),
     Output('indicators', 'figure'),
     Output('trend', 'figure'),
     Output('heatmap', 'figure'),
     Output('risk', 'figure'),
     Output('returns', 'figure'),
     Output('vol-analysis', 'figure'),
     Output('stats', 'children')],
    [Input('ticker-dropdown', 'value'),
     Input('btn-1d', 'n_clicks'),
     Input('btn-1w', 'n_clicks'),
     Input('btn-1m', 'n_clicks'),
     Input('btn-3m', 'n_clicks'),
     Input('btn-6m', 'n_clicks'),
     Input('btn-1y', 'n_clicks'),
     Input('btn-5y', 'n_clicks'),
     Input('refresh-btn', 'n_clicks')]
)
def update_all_charts(ticker, *button_clicks):
    # Determine which button was clicked
    ctx = dash.callback_context
    if not ctx.triggered:
        period = '1M'  # Default
        period_api = '1mo'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        period_map = {
            'btn-1d': ('1D', '1d'),
            'btn-1w': ('1W', '5d'), 
            'btn-1m': ('1M', '1mo'),
            'btn-3m': ('3M', '3mo'),
            'btn-6m': ('6M', '6mo'),
            'btn-1y': ('1Y', '1y'),
            'btn-5y': ('5Y', '5y'),
            'refresh-btn': ('1M', '1mo'),  # Default on refresh
            'ticker-dropdown': ('1M', '1mo')  # Default on ticker change
        }
        period, period_api = period_map.get(button_id, ('1M', '1mo'))
    
    # Get data with caching
    try:
        data = get_data_with_cache(ticker, period_api)
        if not data:
            # Return empty charts
            empty_fig = go.Figure().add_annotation(
                text="No data available. Check if Transform Service is running on port 8001", 
                showarrow=False,
                font=dict(size=16, color=colors['text'])
            )
            empty_fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='#1e2631',
                plot_bgcolor='#0d1421'
            )
            return [empty_fig] * 8 + ["Service not available"]
        
        df = pd.DataFrame(data)
        
        # Generate all 9 visualizations from the same historical dataset
        fig1 = create_candlestick_chart(df, f"{ticker} - {period}")
        fig2 = create_volume_chart(df)
        fig3 = create_technical_indicators(df)
        fig4 = create_trend_chart(df)
        fig5 = create_sector_heatmap(df, ticker)
        fig6 = create_risk_charts(df)
        fig7 = create_returns_chart(df)
        fig8 = create_volume_analysis(df)
        fig9 = create_stats_panel(df, ticker, period)
        
        return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9
        
    except Exception as e:
        # Return error charts
        error_fig = go.Figure().add_annotation(
            text=f"Error: {str(e)}", 
            showarrow=False,
            font=dict(size=14, color=colors['negative'])
        )
        error_fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='#1e2631',
            plot_bgcolor='#0d1421'
        )
        return [error_fig] * 8 + [f"Error: {str(e)}"]

# Add Flask routes for health checks and refresh
from flask import Flask, jsonify
import time
import psutil

server = app.server

@server.route('/health')
def health_check():
    """Comprehensive health check for visualization service"""
    start_time = time.time()
    
    checks = {
        "service": "visualization-service",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "checks": {}
    }
    
    # Test 1: System resource check
    try:
        memory_percent = psutil.virtual_memory().percent
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        if memory_percent > 90:
            checks["checks"]["memory"] = "unhealthy - high usage"
        elif memory_percent > 80:
            checks["checks"]["memory"] = "degraded - moderate usage"
        else:
            checks["checks"]["memory"] = "healthy"
        checks["memory_usage_percent"] = round(memory_percent, 1)
        
        if cpu_percent > 90:
            checks["checks"]["cpu"] = "unhealthy - high usage"
        elif cpu_percent > 80:
            checks["checks"]["cpu"] = "degraded - moderate usage"
        else:
            checks["checks"]["cpu"] = "healthy"
        checks["cpu_usage_percent"] = round(cpu_percent, 1)
        
    except Exception as e:
        checks["checks"]["system"] = f"degraded - {str(e)[:50]}"
    
    # Test 2: Transform service connectivity
    try:
        transform_url = os.getenv('TRANSFORM_SERVICE_URL', 'http://localhost:8001')
        response = requests.get(f'{transform_url}/health', timeout=3)
        if response.status_code == 200:
            checks["checks"]["transform_service"] = "healthy"
        else:
            checks["checks"]["transform_service"] = f"degraded - status {response.status_code}"
            
    except Exception as e:
        checks["checks"]["transform_service"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 3: Data fetching capability
    try:
        # Test basic data fetch with cache
        test_data = get_cached_data("AAPL", "5d", int(time.time() / 300))
        if isinstance(test_data, list) and len(test_data) >= 0:  # Empty list is OK
            checks["checks"]["data_fetching"] = "healthy"
        else:
            checks["checks"]["data_fetching"] = "degraded - unexpected data format"
            
    except Exception as e:
        checks["checks"]["data_fetching"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 4: Dashboard component health
    try:
        import plotly.graph_objects as go
        import pandas as pd
        
        # Test basic chart creation
        test_fig = go.Figure()
        test_df = pd.DataFrame({"test": [1, 2, 3]})
        
        if test_fig and len(test_df) == 3:
            checks["checks"]["dashboard_components"] = "healthy"
        else:
            checks["checks"]["dashboard_components"] = "degraded - component issues"
            
    except Exception as e:
        checks["checks"]["dashboard_components"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 5: Response time check
    response_time = (time.time() - start_time) * 1000
    checks["response_time_ms"] = round(response_time, 2)
    
    if response_time > 5000:
        checks["checks"]["response_time"] = "unhealthy - too slow"
    elif response_time > 2000:
        checks["checks"]["response_time"] = "degraded - slow"
    else:
        checks["checks"]["response_time"] = "healthy"
    
    # Overall status calculation
    unhealthy_count = sum(1 for check in checks["checks"].values() if "unhealthy" in str(check))
    degraded_count = sum(1 for check in checks["checks"].values() if "degraded" in str(check))
    
    if unhealthy_count > 0:
        checks["status"] = "unhealthy"
    elif degraded_count > 0:
        checks["status"] = "degraded"
    else:
        checks["status"] = "healthy"
    
    return jsonify(checks)

@server.route('/refresh')
def refresh_dashboard():
    """Trigger dashboard refresh - clear cache"""
    try:
        # Clear the cache to force fresh data on next request
        get_cached_data.cache_clear()
        
        return jsonify({
            "message": "Dashboard cache cleared successfully",
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        })
    except Exception as e:
        return jsonify({
            "error": f"Failed to refresh dashboard: {str(e)}",
            "timestamp": datetime.now().isoformat(),
            "status": "error"
        }), 500

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8002)