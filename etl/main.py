# ===============================
# main.py — Entry point ETL pipeline
# ===============================

import threading
from etl.kaggle_downloader import ensure_datasets
from etl.cassandra_client import get_session, prepare_statements
from etl.producer import get_producer, stream_loop
from etl.consumer import consume_loop


def main():
    print("🔵 ETL Pipeline avviata\n")

    # 1. Scarica i dataset da Kaggle se non già presenti
    ensure_datasets()

    # 2. Connessioni
    session  = get_session()
    stmts    = prepare_statements(session)
    producer = get_producer()

    # 3. Avvia consumer (daemon) e producer in parallelo
    consumer_thread = threading.Thread(
        target=consume_loop,
        args=(session, stmts),
        name="ConsumerThread",
        daemon=True
    )
    producer_thread = threading.Thread(
        target=stream_loop,
        args=(producer,),
        name="ProducerThread"
    )

    consumer_thread.start()
    producer_thread.start()
    producer_thread.join()


if __name__ == "__main__":
    main()
