
SELECT
    date_update,
    max(amount_total) as amount_total, 
    max(cnt_transactions) as cnt_transactions,
    max(avg_transactions_per_account) as avg_transactions_per_account,
    max(cnt_accounts_make_transactions) as cnt_accounts_make_transactions
FROM IAROSLAVRUSSUYANDEXRU__DWH.global_metrics
    GROUP BY currency_from, date_update