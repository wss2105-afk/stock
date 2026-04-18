import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.utils
import json


def make_main_chart(df, name):
    """캔들차트 + 이동평균선 + 볼린저밴드"""
    fig = make_subplots(rows=5, cols=1, shared_xaxes=True,
                        row_heights=[0.40, 0.15, 0.18, 0.13, 0.14],
                        subplot_titles=[f'{name} 주가', 'RSI', 'MACD', 'MFI', 'Bollinger Band %B'],
                        vertical_spacing=0.04)

    # 캔들차트
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='주가',
        increasing_line_color='#e74c3c', decreasing_line_color='#3498db'
    ), row=1, col=1)

    # Bollinger Bands
    fig.add_trace(go.Scatter(x=df.index, y=df['bb_upper'], name='BB Upper',
                             line=dict(color='rgba(100,180,255,0.7)', dash='dash', width=1.2), showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['bb_lower'], name='BB Lower',
                             fill='tonexty', fillcolor='rgba(100,180,255,0.07)',
                             line=dict(color='rgba(100,180,255,0.7)', dash='dash', width=1.2), showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['bb_mid'], name='BB Mid',
                             line=dict(color='rgba(100,180,255,0.4)', dash='dot', width=1.0), showlegend=False), row=1, col=1)

    # 이동평균선
    colors = {'ma5': '#e74c3c', 'ma20': '#f39c12', 'ma60': '#2ecc71', 'ma115': '#9b59b6'}
    labels = {'ma5': '5일', 'ma20': '20일', 'ma60': '60일', 'ma115': '115일'}
    for col, color in colors.items():
        fig.add_trace(go.Scatter(x=df.index, y=df[col], name=labels[col],
                                 line=dict(color=color, width=1.2)), row=1, col=1)

    # RSI
    fig.add_trace(go.Scatter(x=df.index, y=df['rsi'], name='RSI',
                             line=dict(color='#8e44ad', width=1.5)), row=2, col=1)
    fig.add_hline(y=70, line_color='red', line_dash='dash', row=2, col=1)
    fig.add_hline(y=30, line_color='blue', line_dash='dash', row=2, col=1)

    # MACD
    colors_hist = ['#e74c3c' if v >= 0 else '#3498db' for v in df['macd_hist'].fillna(0)]
    fig.add_trace(go.Bar(x=df.index, y=df['macd_hist'], name='MACD Hist',
                         marker_color=colors_hist, showlegend=False), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['macd'], name='MACD',
                             line=dict(color='#e74c3c', width=1.2)), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['macd_signal'], name='Signal',
                             line=dict(color='#3498db', width=1.2)), row=3, col=1)

    # MFI
    fig.add_trace(go.Scatter(x=df.index, y=df['mfi'], name='MFI',
                             line=dict(color='#16a085', width=1.5)), row=4, col=1)
    fig.add_hline(y=80, line_color='red', line_dash='dash', row=4, col=1)
    fig.add_hline(y=20, line_color='blue', line_dash='dash', row=4, col=1)

    # Bollinger Band %B
    bb_pct = df['bb_pct'] * 100
    bb_colors = ['#ff4d4d' if v > 90 else '#64b4ff' if v < 10 else '#a0a0c0' for v in bb_pct.fillna(50)]
    fig.add_trace(go.Bar(x=df.index, y=bb_pct, name='BB %B',
                         marker_color=bb_colors, showlegend=False), row=5, col=1)
    fig.add_hline(y=100, line_color='#ff4d4d', line_dash='dash', line_width=1, row=5, col=1)
    fig.add_hline(y=0,   line_color='#64b4ff', line_dash='dash', line_width=1, row=5, col=1)
    fig.add_hline(y=50,  line_color='#555566', line_dash='dot',  line_width=1, row=5, col=1)

    fig.update_layout(height=950, template='plotly_white',
                      xaxis_rangeslider_visible=False,
                      legend=dict(orientation='h', y=1.02),
                      margin=dict(l=40, r=20, t=60, b=20))
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def make_ma_chart(df, name):
    """이동평균선 배열 전용 차트 (base64 이미지)"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import io, base64

    fig, ax = plt.subplots(figsize=(6, 3.2))
    fig.patch.set_facecolor('#0f1117')
    ax.set_facecolor('#0f1117')

    # 주가 점선
    ax.plot(df.index, df['close'], color='#ffffff', linewidth=1.0,
            linestyle='dotted', alpha=0.6, label='주가')

    # 이동평균선
    ma_styles = [
        ('ma5',   '#ff4757', '5일'),
        ('ma20',  '#ffd32a', '20일'),
        ('ma60',  '#2ed573', '60일'),
        ('ma115', '#a29bfe', '115일'),
    ]
    for col, color, label in ma_styles:
        if col in df.columns and df[col].notna().any():
            ax.plot(df.index, df[col], color=color, linewidth=1.8, label=label)

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=30, color='#a0aec0', fontsize=7)
    plt.yticks(color='#a0aec0', fontsize=7)
    ax.tick_params(colors='#4a5568')
    for spine in ax.spines.values():
        spine.set_edgecolor('#2d3748')

    ax.legend(loc='upper left', fontsize=8, facecolor='#1a1a2e',
              labelcolor='white', edgecolor='#4a5568', ncol=5)
    plt.tight_layout(pad=0.5)

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, facecolor='#0f1117')
    plt.close(fig)
    buf.seek(0)
    return 'data:image/png;base64,' + base64.b64encode(buf.read()).decode()


def make_supply_zone_chart(zone_df, current_price):
    """매물대 차트"""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=zone_df['volume'], y=zone_df['price_mid'],
        orientation='h', name='매물대',
        marker_color=['#e74c3c' if p >= current_price * 0.98 and p <= current_price * 1.02
                      else '#3498db' for p in zone_df['price_mid']]
    ))
    fig.add_hline(y=current_price, line_color='#f39c12', line_width=2,
                  annotation_text=f'현재가 {current_price:,}원')
    fig.update_layout(title='매물대 분포', height=400, template='plotly_white',
                      xaxis_title='거래량', yaxis_title='가격',
                      margin=dict(l=40, r=20, t=40, b=20))
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def make_investor_chart(investor_df):
    """수급 차트"""
    if investor_df.empty:
        return None

    fig = go.Figure()
    cols_map = {}
    for col in investor_df.columns:
        if '외국인' in col or '외인' in col:
            cols_map['외국인'] = col
        elif '연기금' in col:
            cols_map['연기금'] = col
        elif '금융투자' in col:
            cols_map['금융투자'] = col
        elif '기관' in col and '연기금' not in col and '금융' not in col:
            cols_map['기관합계'] = col
        elif '개인' in col:
            cols_map['개인'] = col

    colors_map = {
        '외국인': '#e74c3c', '기관합계': '#3498db',
        '연기금': '#2ecc71', '금융투자': '#9b59b6', '개인': '#f39c12'
    }

    for label, col in cols_map.items():
        vals = investor_df[col]
        bar_colors = ['#e74c3c' if v > 0 else '#3498db' for v in vals]
        fig.add_trace(go.Bar(x=investor_df.index, y=vals,
                             name=label, marker_color=bar_colors,
                             visible=True if label == '외국인' else 'legendonly'))

    fig.update_layout(title='투자자별 순매수 현황', height=350,
                      template='plotly_white', barmode='group',
                      margin=dict(l=40, r=20, t=40, b=20))
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
