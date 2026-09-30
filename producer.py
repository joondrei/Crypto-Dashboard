import json
import websocket
from datetime import datetime, timezone
import logging
from kafka import KafkaProducer
import signal
import sys

import sys
from pathlib import Path

# Add the parent directory (project root) to the Python path
sys.path.append(str(Path.cwd().parent))
from configloader import ConfigLoader
from streamloader import StreamLoader


# ── LOGGING ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)
topic_name = "crypto.livestream"

streamconfigfile = "../stream_config.json"
stream_loader = StreamLoader(streamconfigfile)

configfile = "../kafka.env"
config = ConfigLoader(configfile)
producer_config = config.to_kafka_dict()

producer = KafkaProducer(
    **producer_config   
)  



ws = None
market_state_cache = {}

def handle_shutdown(signum, frame):
    """Callback function triggered when Ctrl+C or terminal termination is requested"""
    log.info(f"Received signal {signum}. Commencing graceful shutdown...")
    if ws:
        log.info("Closing WebSocket connection...")
        ws.close()


def on_message(ws, message):
    try:
        payload = json.loads(message)
        stream_name = payload.get('stream', '')
        data = payload.get('data')

        if not data or not stream_name:
            return

        symbol = data.get('s').upper()
        
        if symbol not in market_state_cache:
            market_state_cache[symbol] = {
                'symbol': symbol,
                'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
                'bid_price': 0.0,
                'ask_price': 0.0,
                'spread': 0.0,
                'last_price': 0.0,
                'pct_change': 0.0
            }
        
        state = market_state_cache[symbol]
        
        if "bookTicker" in stream_name:
            bid_price = float(data.get('b', 0))
            ask_price = float(data.get('a', 0))
            
            state['bid_price'] = bid_price
            state['ask_price'] = ask_price
            state['spread'] = round(ask_price - bid_price, 2)
            state['timestamp'] = int(datetime.now(timezone.utc).timestamp() * 1000)

        elif "ticker" in stream_name:
            state['last_price'] = float(data.get('c', 0))
            state['pct_change'] = float(data.get('P', 0))
            if data.get('E'):
                state['timestamp'] = int(data.get('E')) 
        else:
            return

        flat_complete_record = state.copy()

        send_to_kafka(flat_complete_record, triggered_by="bookTicker" if "bookTicker" in stream_name else "ticker")

    except Exception as e:
        log.error(f"Error merging stream states: {e}")

def on_error(ws, error):
    log.error(f"Error establishing websocket connection: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to Binance Combined Streams API")

def send_to_kafka(data, triggered_by):
    """Send fully filled, zero-null message to Kafka with symbol as partition key"""
    try:
        partition_key = data['symbol']
        
        producer.send(
            topic=topic_name,
            key=partition_key.encode('utf-8'),
            value=json.dumps(data).encode('utf-8')
        )
        
        if triggered_by == 'bookTicker':
            log.info(f"[bookTicker Event] Published complete state for {partition_key} | Spread: ${data['spread']:.2f}")
        else:
            log.info(f"[ticker Event] Published complete state for {partition_key} | Last: ${data['last_price']:.2f} ({data['pct_change']}%)")
        
    except Exception as e:
        log.error(f"Failed to send complete record to Kafka: {e}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        generated_url = stream_loader.generate_url()
        log.info(f"Targeting URL: {generated_url}")
    except Exception as e:
        log.critical(f"Failed to compile streams URL from config: {e}")
        sys.exit(1)

    ws = websocket.WebSocketApp(
        generated_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    log.info("Starting WebSocket execution loop...")
    ws.run_forever()

    log.info("Flushing remaining Kafka messages...")
    producer.flush(timeout=5)

    log.info("Closing Kafka producer connection...")
    producer.close(timeout=5)
    
    log.info("Shutdown sequence complete. Exiting application safely.")
    sys.exit(0)
