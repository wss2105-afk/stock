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
        def fmt_vol(v):
            if v >= 100_000_000:
                return f'{v/100_000_000:.1f}억'
            elif v >= 10_000:
                return f'{v/10_000:.0f}만'
            return f'{v:,.0f}'

        vol_labels = [fmt_vol(v) for v in supply_df['volume']]

        fig.add_trace(go.Bar(
            x=supply_df['volume'],
            y=supply_df['price_mid'],
            orientation='h',
            name='매물대',
            marker=dict(color=bar_colors, line_width=0),
            text=vol_labels,
            textposition='outside',
            textfont=dict(size=8, color='rgba(200,200,200,0.75)'),
            cliponaxis=False,
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
        margin=dict(l=40, r=36, t=60, b=20),
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
    """외인·기관별 순매수 차트 — 최근 20거래일, 2패널"""
    if investor_df.empty:
        return None

    import pandas as pd
    df = investor_df.tail(20).copy()

    def find_col(df, *keywords):
        for kw in keywords:
            for c in df.columns:
                if kw in str(c):
                    return c
        return None

    foreign_col = find_col(df, '외국인합계', '외국인', '외인')
    pension_col = find_col(df, '연기금')
    finance_col = find_col(df, '금융투자')
    insure_col  = find_col(df, '보험')
    trust_col   = find_col(df, '투신')
    private_col = find_col(df, '사모')
    bank_col    = find_col(df, '은행')
    other_fin   = find_col(df, '기타금융')

    inst_cfg = [
        (pension_col, '연기금',   '#2ecc71'),
        (finance_col, '금융투자', '#9b59b6'),
        (insure_col,  '보험',     '#f39c12'),
        (trust_col,   '투신',     '#1abc9c'),
        (private_col, '사모',     '#e67e22'),
        (bank_col,    '은행',     '#95a5a6'),
        (other_fin,   '기타금융', '#7f8c8d'),
    ]
    # 실제 데이터 있는 것만
    inst_cfg = [(c, l, clr) for c, l, clr in inst_cfg
                if c and not df[c].fillna(0).eq(0).all()]

    has_inst = len(inst_cfg) > 0

    if has_inst:
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.42, 0.58],
            vertical_spacing=0.06,
            subplot_titles=['외국인 순매수 (주)', '기관 세부 순매수 (주)'],
        )
    else:
        fig = make_subplots(rows=1, cols=1)

    x = df.index

    # ── 외국인 (Row 1) ──────────────────────────────────────
    if foreign_col:
        vals = df[foreign_col].fillna(0)
        cum  = vals.cumsum()
        fig.add_trace(go.Bar(
            x=x, y=vals, name='외국인',
            marker_color=['#e74c3c' if v >= 0 else '#3498db' for v in vals],
            hovertemplate='%{x|%m/%d}<br>외국인: %{y:+,.0f}주<extra></extra>',
        ), row=1, col=1)
        # 누적선
        fig.add_trace(go.Scatter(
            x=x, y=cum, name='외국인 누적',
            line=dict(color='rgba(231,76,60,0.6)', width=1.5, dash='dot'),
            yaxis='y2' if not has_inst else None,
            hovertemplate='%{x|%m/%d}<br>누적: %{y:+,.0f}주<extra></extra>',
            showlegend=False,
        ), row=1, col=1)

    # ── 기관 세부 (Row 2, 스택) ──────────────────────────────
    if has_inst:
        for col, label, color in inst_cfg:
            vals = df[col].fillna(0)
            fig.add_trace(go.Bar(
                x=x, y=vals, name=label,
                marker_color=color,
                hovertemplate=f'%{{x|%m/%d}}<br>{label}: %{{y:+,.0f}}주<extra></extra>',
            ), row=2, col=1)

        # 기관합계 라인 오버레이
        inst_total = sum(df[c].fillna(0) for c, _, _ in inst_cfg)
        fig.add_trace(go.Scatter(
            x=x, y=inst_total.cumsum(), name='기관 누적',
            line=dict(color='rgba(255,192,0,0.7)', width=1.5, dash='dot'),
            hovertemplate='%{x|%m/%d}<br>기관누적: %{y:+,.0f}주<extra></extra>',
            showlegend=False,
        ), row=2, col=1)

    rows = 2 if has_inst else 1
    fig.update_layout(
        height=480 if has_inst else 280,
        barmode='relative',
        hovermode='x unified',
        legend=dict(orientation='h', y=-0.1, x=0,
                    font=dict(size=10), bgcolor='rgba(0,0,0,0)'),
        margin=dict(l=60, r=20, t=40, b=70),
    )
    fig.update_xaxes(tickformat='%m/%d', tickangle=-45)
    fig.update_yaxes(tickformat=',.0f', zeroline=True,
                     zerolinecolor='rgba(255,255,255,0.15)', zerolinewidth=1)

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
