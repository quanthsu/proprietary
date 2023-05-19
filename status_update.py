from tsdb_client import TSDBClient
import pandas as pd
import trading_calendars as tc
import pytz

xtai = tc.get_calendar("XTAI")
import datetime

from utils.Config import EnvConfig

cli = TSDBClient(
    host=EnvConfig.DB_HOST,
    port=EnvConfig.DB_PORT,
    user=EnvConfig.DB_USER,
    password=EnvConfig.DB_PASSWORD,
    db=EnvConfig.DB_DATABASE,
)


strategy_df = cli.execute_query(
    """
SELECT * FROM dealer.strategy
""",
    out_type="df",
)

trade_df = cli.execute_query(
    """
    select 
        t_st.name as 策略, 
        t_pos.code as 代碼,
        t_pos.position as 部位,
        t_pos.avg_price as 成本價,
        t_quote.close as 收盤價,
        (position * (close - avg_price))*1000 as 損益, 
        (close / avg_price - 1) * 100 as "損益(%)",
        t_pos.trade_date

    from (
        select 
            strategy, 
            code,
            trade_date,
            sum((case when action = 'B' then 1 else -1 end) * qty) as position,
            sum(price*qty) / sum(qty) as avg_price
        from dealer.trades
        group by strategy, code, trade_date
    )t_pos
left join public.quote_snapshots t_quote on t_pos.code = t_quote.code
left join dealer.strategy t_st on t_pos.strategy = t_st.id 

order by t_pos.strategy, t_pos.code
""",
    out_type="df",
)


status_list = []

for i in range(len(trade_df)):
    if datetime.date(
        datetime.datetime.now().year,
        datetime.datetime.now().month,
        datetime.datetime.now().day,
    ) == datetime.date(
        trade_df.iloc[i]["trade_date"].year,
        trade_df.iloc[i]["trade_date"].month,
        trade_df.iloc[i]["trade_date"].day,
    ):
        strategy_id = int(
            strategy_df[strategy_df["name"] == trade_df.iloc[i]["策略"]]["id"]
        )
        holding_period = int(
            strategy_df[strategy_df["name"] == trade_df.iloc[i]["策略"]]["holding_period"]
        )
        in_date = trade_df.iloc[i]["trade_date"]
        # out_date = datetime.datetime.date(
        #     xtai.sessions_window(
        #         pd.Timestamp(in_date.strftime("%Y-%m-%d")), holding_period
        #     )[-1]
        # )
        # TODO: temporarily use pd.offsets.BDay
        out_date = (in_date + holding_period*pd.offsets.BDay()).to_pydatetime().date()

        status_list.append(
            [
                strategy_id,
                "S",
                trade_df.iloc[i]["代碼"],
                trade_df.iloc[i]["成本價"],
                trade_df.iloc[i]["部位"],
                in_date,
                out_date,
            ]
        )

cli.execute_values_df(
    df=pd.DataFrame(
        status_list,
        columns=[
            "strategy",
            "security_type",
            "code",
            "cost",
            "qty",
            "in_date",
            "out_date",
        ],
    ),
    table="dealer.status",
)
