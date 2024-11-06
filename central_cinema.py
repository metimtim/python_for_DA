from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta


def calculate_metrics(pg_user, pg_password, pg_host, today):
    today_str = today.strftime('%Y-%m-%d')
    conn = psycopg2.connect(
        dbname='courses'
        , user=pg_user  # твой юзер
        , password=pg_password  # твой пароль
        , target_session_attrs='read-write'
        , host=pg_host
        , port='5432'

    )

    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_users")
    users_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    users = pd.DataFrame(users_sql, columns=colnames)

    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_user_payments")
    user_payments_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    user_payments = pd.DataFrame(user_payments_sql, columns=colnames)

    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_partner_commission")
    partner_comission_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    partner_comission = pd.DataFrame(partner_comission_sql, columns=colnames)

    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_user_activity where play_start>'2024-08-01'")
    user_activity_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    user_activity = pd.DataFrame(user_activity_sql, columns=colnames)

    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_title")
    title_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    title = pd.DataFrame(title_sql, columns=colnames)

    yesterday = datetime.strptime((today - timedelta(days=1)).strftime('%Y-%m-%d'), '%Y-%m-%d')
    yesterday_last_month = datetime.strptime((today - relativedelta(months=1) - timedelta(days=1)).strftime('%Y-%m-%d'),
                                             '%Y-%m-%d')
    yesterday_prev_month = datetime.strptime((today - relativedelta(months=2) - timedelta(days=1)).strftime('%Y-%m-%d'),
                                             '%Y-%m-%d')

    user_payments['payment_day'] = pd.to_datetime(user_payments['payment_date']).dt.to_period('D').dt.to_timestamp()

    trial_yesterday = user_payments[(user_payments['is_trial'] == 1) & (user_payments['payment_day'] == yesterday)][
        'user_payment_id'].count()

    trial_yesterday_last_month = \
        user_payments[(user_payments['is_trial'] == 1) & (user_payments['payment_day'] == yesterday_last_month)][
            'user_payment_id'].count()

    trial_delta = round((trial_yesterday) / trial_yesterday_last_month * 100, 2)

    symbol_trial_delta = "\U0001F4C8" + "\U00002705" if trial_delta > 100 else "\U0001F4C8 \U0001F53B"


    pays_yesterday = user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day'] == yesterday)][
        'user_payment_id'].count()

    pays_yesterday_last_month = \
        user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day'] == yesterday_last_month)][
            'user_payment_id'].count()

    pays_delta = round((pays_yesterday) / pays_yesterday_last_month * 100, 2)

    symbol_pays_delta = "\U0001F4C8" + "\U00002705" if pays_delta > 100 else "\U0001F4C8 \U0001F53B"



    conv_yesterday1 = user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day'] == yesterday)]
    conv_yesterday2 = user_payments[
        (user_payments['is_trial'] == 1) & (user_payments['payment_day'] == yesterday_last_month)]
    conv_yesterday = conv_yesterday1.merge(conv_yesterday2, on='user_id', how='inner')
    conv_yesterday = conv_yesterday['user_id'].count() / conv_yesterday2['user_id'].count()
    conv_yesterday_last_month1 = user_payments[
        (user_payments['is_trial'] == 0) & (user_payments['payment_day'] == yesterday_last_month)]
    conv_yesterday_last_month2 = user_payments[
        (user_payments['is_trial'] == 1) & (user_payments['payment_day'] == yesterday_prev_month)]
    conv_yesterday_last_month = conv_yesterday_last_month1.merge(conv_yesterday_last_month2, on='user_id', how='inner')
    conv_yesterday_last_month = conv_yesterday_last_month['user_id'].count() / conv_yesterday_last_month2['user_id'].count()

    conv_delta = round((conv_yesterday) / conv_yesterday_last_month * 100, 2)

    symbol_conv_delta = "\U0001F4C8" + "\U00002705" if conv_delta > 100 else "\U0001F4C8 \U0001F53B"


    cash_yesterday = user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day'] == yesterday)][
                         'user_payment_id'].count() * 299

    cash_yesterday_last_month = \
        user_payments[(user_payments['is_trial'] == 0) & (user_payments['payment_day'] == yesterday_last_month)][
            'user_payment_id'].count() * 299

    cash_delta = round((cash_yesterday) / cash_yesterday_last_month * 100, 2)

    symbol_cash_delta = "\U0001F4C8" + "\U00002705" if cash_delta > 100 else "\U0001F4C8 \U0001F53B"


    cac = partner_comission.merge(user_payments, on='partner_id')
    cac_yesterday = cac[(cac['is_trial'] == 1) & (cac['payment_day'] == yesterday)]['commission'].mean()
    cac_yesterday_last_month = cac[(cac['is_trial'] == 1) & (cac['payment_day'] == yesterday_last_month)][
        'commission'].mean()

    cac_delta = round((cac_yesterday) / cac_yesterday_last_month * 100, 2)

    symbol_cac_delta = "\U0001F4C8" + "\U00002705" if cac_delta < 100 else "\U0001F4C8 \U0001F53B"


    def get_avg_lt(date):
        retention = user_payments.merge(users, on='user_id', how='inner')
        retention = retention[retention['payment_day'] <= date]
        retention = retention.groupby('user_id').agg(cnt=('user_id', 'count')).reset_index()
        retention = retention.merge(users, on='user_id')
        retention = retention[['user_id', 'cnt', 'cogort']]
        df_coh = retention.groupby('cogort').agg(cnt_all=('cnt', lambda x: (x >= 1).sum()),
                                                 cnt_1=('cnt', lambda x: (x >= 2).sum()),
                                                 cnt_2=('cnt', lambda x: (x >= 3).sum()),
                                                 cnt_3=('cnt', lambda x: (x >= 4).sum()),
                                                 cnt_4=('cnt', lambda x: (x >= 5).sum()),
                                                 cnt_5=('cnt', lambda x: (x >= 6).sum()),
                                                 cnt_6=('cnt', lambda x: (x >= 7).sum()),
                                                 cnt_7=('cnt', lambda x: (x >= 8).sum()),
                                                 cnt_8=('cnt', lambda x: (x >= 9).sum()),
                                                 cnt_9=('cnt', lambda x: (x >= 10).sum()),
                                                 cnt_10=('cnt', lambda x: (x >= 11).sum()),
                                                 cnt_11=('cnt', lambda x: (x >= 12).sum()),
                                                 cnt_12=('cnt', lambda x: (x >= 13).sum()),
                                                 cnt_13=('cnt', lambda x: (x >= 14).sum()),
                                                 cnt_14=('cnt', lambda x: (x >= 15).sum()),
                                                 cnt_15=('cnt', lambda x: (x >= 16).sum())
                                                 ).reset_index()
        df_coh['ret_1'] = df_coh['cnt_1'] / df_coh['cnt_all']
        df_coh['ret_2'] = df_coh['cnt_2'] / df_coh['cnt_all']
        df_coh['ret_3'] = df_coh['cnt_3'] / df_coh['cnt_all']
        df_coh['ret_4'] = df_coh['cnt_4'] / df_coh['cnt_all']
        df_coh['ret_5'] = df_coh['cnt_5'] / df_coh['cnt_all']
        df_coh['ret_6'] = df_coh['cnt_6'] / df_coh['cnt_all']
        df_coh['ret_7'] = df_coh['cnt_7'] / df_coh['cnt_all']
        df_coh['ret_8'] = df_coh['cnt_8'] / df_coh['cnt_all']
        df_coh['ret_9'] = df_coh['cnt_9'] / df_coh['cnt_all']
        df_coh['ret_10'] = df_coh['cnt_10'] / df_coh['cnt_all']
        df_coh['ret_11'] = df_coh['cnt_11'] / df_coh['cnt_all']
        df_coh['ret_12'] = df_coh['cnt_12'] / df_coh['cnt_all']
        df_coh['ret_13'] = df_coh['cnt_13'] / df_coh['cnt_all']
        df_coh['ret_14'] = df_coh['cnt_14'] / df_coh['cnt_all']
        df_coh['ret_15'] = df_coh['cnt_15'] / df_coh['cnt_all']

        df_coh = df_coh[df_coh['ret_5'] != 0.0]
        df_coh['lt'] = df_coh['ret_1'] + df_coh['ret_2'] + df_coh['ret_3'] + df_coh['ret_4'] + df_coh['ret_5'] + df_coh[
            'ret_6'] + df_coh['ret_7'] + df_coh['ret_8'] + df_coh['ret_9'] + df_coh['ret_10'] + df_coh['ret_11'] + \
                       df_coh[
                           'ret_12'] + df_coh['ret_13'] + df_coh['ret_14'] + df_coh['ret_15']
        return df_coh['lt'].mean()

    ltr_yesterday = get_avg_lt(yesterday) * 299
    ltr_yesterday_last_month = get_avg_lt(yesterday_last_month) * 299

    ltr_delta = round((ltr_yesterday) / ltr_yesterday_last_month * 100, 2)

    symbol_ltr_delta = "\U0001F4C8" + "\U00002705" if ltr_delta > 100 else "\U0001F4C8 \U0001F53B"


    cac_yesterday_whole = cac[(cac['is_trial'] == 1) & (cac['payment_day'] <= yesterday)]['commission'].mean()
    cac_yesterday_last_month_whole = cac[(cac['is_trial'] == 1) & (cac['payment_day'] <= yesterday_last_month)][
        'commission'].mean()
    ltv_yesterday = ltr_yesterday - cac_yesterday_whole
    ltv_yesterday_last_month = ltr_yesterday_last_month - cac_yesterday_last_month_whole
    ltv_delta = round((ltv_yesterday) / ltv_yesterday_last_month * 100, 2)

    symbol_ltv_delta = "\U0001F4C8" + "\U00002705" if ltv_delta > 100 else "\U0001F4C8 \U0001F53B"


    user_activity['play_end_day'] = pd.to_datetime(user_activity['play_end']).dt.to_period('D').dt.to_timestamp()
    user_activity['play_start_day'] = pd.to_datetime(user_activity['play_start']).dt.to_period('D').dt.to_timestamp()
    user_activity['session'] = user_activity['play_end'] - user_activity['play_start']
    user_activity['session'] = user_activity['session'].dt.total_seconds() / 60
    session_yesterday = \
        user_activity[(user_activity['play_start_day'] == yesterday) | (user_activity['play_end_day'] == yesterday)][
            'session'].mean()
    session_yesterday_last_month = user_activity[(user_activity['play_start_day'] == yesterday_last_month) | (
            user_activity['play_end_day'] == yesterday_last_month)]['session'].mean()
    session_delta = round((session_yesterday) / session_yesterday_last_month * 100, 2)

    symbol_session_delta = "\U0001F4C8" + "\U00002705" if session_delta > 100 else "\U0001F4C8 \U0001F53B"


    watching = user_activity.merge(title, on='title_id', how='inner')
    watching['div_session'] = watching['session'] / watching['duration']
    watching_yesterday = watching[(watching['play_start_day'] == yesterday) | (watching['play_end_day'] == yesterday)][
        'div_session'].mean()
    watching_yesterday_last_month = \
        watching[
            (watching['play_start_day'] == yesterday_last_month) | (watching['play_end_day'] == yesterday_last_month)][
            'div_session'].mean()
    watching_delta = round((watching_yesterday) / watching_yesterday_last_month * 100, 2)

    symbol_watching_delta = "\U0001F4C8" + "\U00002705" if watching_delta > 100 else "\U0001F4C8 \U0001F53B"


    unique_yesterday = watching[(watching['play_start_day'] == yesterday) | (watching['play_end_day'] == yesterday)][
        'user_activity_id'].nunique()
    unique_yesterday_last_month = \
        watching[
            (watching['play_start_day'] == yesterday_last_month) | (watching['play_end_day'] == yesterday_last_month)][
            'user_activity_id'].nunique()

    unique_delta = round((unique_yesterday) / unique_yesterday_last_month * 100, 2)
    symbol_unique_delta = "\U0001F4C8" + "\U00002705" if unique_delta > 100 else "\U0001F4C8 \U0001F53B"


    return f'''
    Central Cinema \U0001F37F
на {today_str}
вчера (в прошлом месяце)
    
\U0001F539Количество триалов
{trial_yesterday} ({trial_yesterday_last_month})
МoМ %: {symbol_trial_delta} {trial_delta}%

\U0001F539Количество оплат
{pays_yesterday} ({pays_yesterday_last_month})
MoM %: {symbol_pays_delta} {pays_delta}%
    
\U0001F539Конверсия в первую оплату%
{conv_yesterday:.2f} ({conv_yesterday_last_month:.2f})
MoM %: {symbol_conv_delta} {conv_delta}%
    
\U0001F4B0Валовый cash-in
{cash_yesterday} ({cash_yesterday_last_month})
MoM %: {symbol_cash_delta} {cash_delta}%
    
\U0001F4B0CAC
{cac_yesterday:.2f} ({cac_yesterday_last_month:.2f})
MoM %: {symbol_cac_delta} {cac_delta}%
    
\U0001F4B0Прогнозируемый LTR
{ltr_yesterday:.0f} ({ltr_yesterday_last_month:.0f})
MoM %: {symbol_ltr_delta} {ltr_delta}%
    
\U0001F4B0Прогнозируемый LTV
{ltv_yesterday:.2f} ({ltv_yesterday_last_month:.2f})
MoM %: {symbol_ltv_delta} {ltv_delta}%
    
\U000023F0Среднее время сессии(мин.)
{session_yesterday:.2f} ({session_yesterday_last_month:.2f})
MoM %: {symbol_session_delta} {session_delta}% 
    
\U000023F0Досматриваемость
{watching_yesterday:.2f} ({watching_yesterday_last_month:.2f})
MoM %: {symbol_watching_delta} {watching_delta}%
'''


