import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.utils
import json


def make_main_chart(df, name, supply_df=None, current_price=None):
    """캔들차트 + 볼린저밴드 + 이동평균선 (우측 매물대 포함)"""
    has_supply = (supply_df is not None and not supply_df.empty
                  and current_price is not None)

    if has_supply:
        fig = make_subplots(
            rows=1, cols=2,
            shared_yaxes=True,
            column_widths=[0.8, 0.2],
            horizontal_spacing=0.004,
        )
    else:
        fig = make_subplots(rows=1, cols=1)

    # 캔들차트
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='주가',
        increasing_line_color='#e74c3c', decreasing_line_color='#3498db'
    ), row=1, col=1)

    # Bollinger Bands
    fig.add_trace(go.Scatter(x=df.index, y=df['bb_upper'], name='BB Upper',
                             line=dict(color='rgba(100,180,255,0.7)', dash='dash', width=1.2),
                             showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['bb_lower'], name='BB Lower',
                             fill='tonexty', fillcolor='rgba(100,180,255,0.07)',
                             line=dict(color='rgba(100,180,255,0.7)', dash='dash', width=1.2),
                             showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['bb_mid'], name='BB Mid',
                             line=dict(color='rgba(100,180,255,0.4)', dash='dot', width=1.0),
                             showlegend=False), row=1, col=1)

    # 이동평균선 (5일, 20일)
    for col_name, color, label in [('ma5','#e74c3c','5일'), ('ma20','#f39c12','20일')]:
        fig.add_trace(go.Scatter(x=df.index, y=df[col_name], name=label,
                                 line=dict(color=color, width=1.2)), row=1, col=1)

    if has_supply:
        # 현재가 ±3% 내 구간 강조
        bar_colors = [
            'rgba(231,76,60,0.80)' if abs(p - current_price) / current_price < 0.03
            else 'rgba(52,152,219,0.45)'
            for p in supply_df['price_mid']
        ]
        fig.add_trace(go.Bar(
            x=supply_df['volume'],
            y=supply_df['price_mid'],
            orientation='h',
            name='매물대',
            marker=dict(color=bar_colors, line_width=0),
            showlegend=False,
            hovertemplate='%{y:,.0f}원<br>거래량: %{x:,.0f}<extra></extra>',
        ), row=1, col=2)

        # 현재가 수평선 (두 패널 공통)
        fig.add_hline(y=current_price,
                      line=dict(color='#FFC000', width=1.2, dash='dot'))

        fig.update_xaxes(showticklabels=False, showgrid=False,
                         zeroline=False, row=1, col=2)
        fig.update_yaxes(showticklabels=False, showgrid=False, row=1, col=2)

    fig.update_layout(
        height=550,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation='h', y=1.02),
        margin=dict(l=40, r=8, t=60, b=20),
        bargap=0.08,
    )
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def make_ma_chart(df, name):
    """이동평균선 배열 차트 (Plotly 인터랙티브)"""
    import pandas as pd

    display_df = df.tail(180) if len(df) > 180 else df
    last = df.iloc[-1]
    cur = last['close']

    ma_cfg = [
        ('ma5',   '#ff4757', '5일'),
        ('ma20',  '#ffd32a', '20일'),
        ('ma60',  '#2ed573', '60일'),
        ('ma120', '#a29bfe', '120일'),
    ]

    fig = go.Figure()

    # 주가 캔들 대신 종가 라인 (얇게)
    fig.add_trace(go.Scatter(
        x=display_df.index,
        y=display_df['close'],
        name='종가',
        line=dict(color='rgba(255,255,255,0.3)', width=1.0),
        hovertemplate='%{x|%m/%d} 종가: %{y:,.0f}원<extra></extra>'
    ))

    # 이동평균선
    for col, color, label in ma_cfg:
        if col not in display_df.columns:
            continue
        series = display_df[col].dropna()
        if series.empty:
            continue
        ma_val = last[col]
        if pd.isna(ma_val):
            fig.add_trace(go.Scatter(
                x=series.index, y=series,
                name=f'{label}선 (데이터 부족)',
                line=dict(color=color, width=2.0, dash='dot'),
                hovertemplate=f'{label}선: %{{y:,.0f}}원<extra></extra>'
            ))
            continue
        diff_pct = (cur - ma_val) / ma_val * 100
        sign = '+' if diff_pct >= 0 else ''
        fig.add_trace(go.Scatter(
            x=series.index, y=series,
            name=f'{label}선  {sign}{diff_pct:.1f}%',
            line=dict(color=color, width=2.2),
            hovertemplate=f'{label}선: %{{y:,.0f}}원<extra></extra>'
        ))
        # 우측 끝 현재값 주석
        fig.add_annotation(
            x=display_df.index[-1], y=ma_val,
            text=f'{int(ma_val):,}',
            showarrow=False,
            font=dict(size=9, color=color),
            xanchor='left', xshift=8, yanchor='middle'
        )

    # 현재가 수평선
    fig.add_hline(
        y=cur,
        line=dict(color='#FFC000', width=1.5, dash='dot'),
        annotation_text=f'현재 {int(cur):,}원',
        annotation_font=dict(color='#FFC000', size=10),
        annotation_position='top left'
    )

    fig.update_layout(
        height=380,
        xaxis_rangeslider_visible=False,
        hovermode='x unified',
        legend=dict(
            orientation='h', y=-0.18, x=0,
            font=dict(size=10),
            bgcolor='rgba(0,0,0,0)',
        ),
        margin=dict(l=50, r=70, t=20, b=60),
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


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
