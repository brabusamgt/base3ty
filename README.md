# base3tyimport time
import sqlite3
from web3 import Web3

RPC_URL = "https://mainnet.base.org"
DB_FILE = "base_indexer.db"


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    with open("schema.sql", "r") as f:
        cursor.executescript(f.read())

    cursor.execute("INSERT OR IGNORE INTO state (id, last_block) VALUES (1, NULL)")
    conn.commit()
    return conn


def get_last_block(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT last_block FROM state WHERE id = 1")
    result = cursor.fetchone()
    return result[0]


def set_last_block(conn, block_number):
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE state SET last_block = ? WHERE id = 1",
        (block_number,),
    )
    conn.commit()


def save_block(conn, block):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO blocks (block_number, block_hash, timestamp, tx_count)
        VALUES (?, ?, ?, ?)
        """,
        (
            block.number,
            block.hash.hex(),
            block.timestamp,
            len(block.transactions),
        ),
    )
    conn.commit()


def main():
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to Base RPC")

    print("Connected to Base")

    conn = init_db()

    last_processed = get_last_block(conn)

    if last_processed is None:
        last_processed = w3.eth.block_number
        set_last_block(conn, last_processed)
        print(f"Initialized at block {last_processed}")
        return

    print(f"Resuming from block {last_processed}")

    while True:
        latest = w3.eth.block_number

        if latest > last_processed:
            for b in range(last_processed + 1, latest + 1):
                block = w3.eth.get_block(b)
                save_block(conn, block)
                set_last_block(conn, b)
                print(f"Indexed block {b}")

            last_processed = latest

        time.sleep(2)


if __name__ == "__main__":
    main()
