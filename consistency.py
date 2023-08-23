from pyflink.table import (EnvironmentSettings, TableEnvironment)


def consistency():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())


    t_env.execute_sql("""
        CREATE TABLE random_source (
            id BIGINT, 
            amount TINYINT,
            sender TINYINT,
            receiver TINYINT,
            event_ts TIMESTAMP(3),
            ingest_ts TIME,
            WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'fields.id.kind'='sequence',
            'fields.id.start'='1',
            'fields.id.end'='10000000',
            'fields.amount.kind'='random',
            'fields.amount.max'='1',
            'fields.amount.min'='1',
            'fields.sender.kind'='random',
            'fields.sender.min'='1',
            'fields.sender.max'='10',
            'fields.receiver.kind'='random',
            'fields.receiver.min'='1',
            'fields.receiver.max'='10',
            'fields.event_ts.max-past'='5000'
        )
    """)

    # t_env.execute_sql("""
    #     CREATE TABLE print_sink_0 (
    #         id BIGINT, 
    #         amount TINYINT,
    #         sender TINYINT,
    #         receiver TINYINT,
    #         event_ts TIMESTAMP,
    #         ingest_ts TIME
    #     ) WITH (
    #         'connector' = 'print'
    #     )
    # """)

    # t_env.execute_sql("""
    #     INSERT INTO print_sink_0
    #         SELECT id, amount, sender, receiver, event_ts, ingest_ts FROM random_source
    # """)

    t_env.execute_sql("""
        CREATE VIEW credit AS
            SELECT
                receiver as account,
                sum(amount) as credits
            FROM
                random_source
            GROUP BY
                receiver
    """)


    t_env.execute_sql("""
        CREATE VIEW debit AS
            SELECT
                sender as account,
                sum(amount) as debits
            FROM
                random_source
            GROUP BY
                sender
    """)

    t_env.execute_sql("""
        CREATE VIEW flows AS
            SELECT
                credit.account as account,
                credit.credits - debit.debits as flow
            FROM
                debit
            INNER JOIN
                credit
            ON
                credit.account = debit.account
    """)

    t_env.execute_sql("""
        CREATE VIEW net AS
            SELECT
                sum(flow) as net_flow
            FROM
                flows
    """)



    t_env.execute_sql("""
        CREATE TABLE print_sink (
            net_flow TINYINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO print_sink
            SELECT net_flow FROM net
    """).wait()


if __name__ == '__main__':

    consistency()
