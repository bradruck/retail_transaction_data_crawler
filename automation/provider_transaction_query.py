# provider_transaction_query module
# Module holds the class => ProviderTransaction - manages Hive query template
# Class responsible to populate the query with api sourced variables
#


class ProviderTransaction(object):

    @staticmethod
    def max_transact_date_query(provider_id, post_period_end_date, start_date):
        query = """
        set hive.execution.engine = tez;
        set fs.s3n.block.size=128000000;
        set fs.s3a.block.size=128000000;

        select datediff(to_date('{pp_end_date}'), to_date('{start_date}'))+1 as day_count,
        count(DISTINCT txn_date) as distinct_txn_count,
        MIN(txn_date) AS min_date,
        MAX(txn_date) AS max_date
        from (select cast(txn_dt as date) as txn_date
        from core_shared.transaction
        WHERE provider_id IN ({pid})
        AND txn_type = 'P'
        AND txn_dt >=('{start_date}')
        group by cast(txn_dt as date)
        ) a
        """.format(pid=provider_id, pp_end_date=post_period_end_date, start_date=start_date)
        return query
