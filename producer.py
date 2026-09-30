import json
import websocket
from datetime import datetime, timezone
import logging
from kafka import KafkaProducer
import signal
import sys
from configloader import ConfigLoader
from streamloader import StreamLoader


# ── LOGGING ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)
topic_name = "crypto.livestream"

streamconfigfile = "stream_config.json"
stream_loader = StreamLoader(streamconfigfile)

configfile = "kafka.env"
config = ConfigLoader(configfile)
producer_config = config.to_kafka_dict()

producer = KafkaProducer(
    **producer_config   
)  



ws = None

def handle_shutdown(signum, frame):
    """Callback function triggered when Ctrl+C or terminal termination is requested"""
    log.info(f"Received signal {signum}. Commencing graceful shutdown...")
    if ws:
        log.info("Closing WebSocket connection...")
        ws.close()


def on_message(ws, message):

    try:
            
        payload = json.loads(message)
        
        stream_name = payload.get('stream')
        data = payload.get('data')

        if not data or not stream_name:
            return

        kafka_payload = {}

        if "bookTicker" in stream_name:
            symbol = data.get('s')  # e.g. "BTCUSDT"
            bid_price = float(data.get('b', 0))
            ask_price = float(data.get('a', 0))
            bid_qty = float(data.get('B', 0))
            ask_qty = float(data.get('A', 0))
            spread = ask_price - bid_price

            kafka_payload = {
                'symbol': symbol,
                'stream_type': 'bookTicker',
                'bid_price': bid_price,
                'bid_qty': bid_qty,
                'ask_price': ask_price,
                'ask_qty': ask_qty,
                'spread': round(spread, 2),
                'event_time': datetime.now(timezone.utc).isoformat()
            }

        elif "ticker" in stream_name:
            symbol = data.get('s')
            last_price = float(data.get('c', 0))
            pct_change = float(data.get('P', 0))
            timestamp = data.get('E')

            try:
                event_time = datetime.fromtimestamp(
                    timestamp / 1000, tz=timezone.utc
                ).isoformat()
            except Exception as e:
                log.error(f"Error converting timestamp {timestamp}: {e}")
                event_time = datetime.now(timezone.utc).isoformat()

            kafka_payload = {
                'symbol': symbol,
                'stream_type': 'ticker',
                'last_price': last_price,
                'pct_change': pct_change,
                'event_time': event_time
            }

        else:
            log.warning(f"Unhandled stream type: {stream_name}")
            return

        if kafka_payload:
            send_to_kafka(kafka_payload)

    except Exception as e:
        log.error(f"Error processing message: {e}")

def on_error(ws, error):
    log.error(f"Error establishing websocket connection: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to Binance Combined Streams API")

def send_to_kafka(data):
    """Send message to Kafka with product_id as partition key"""
    try:
        partition_key = data['symbol']
        
        producer.send(
            topic=topic_name,
            key=partition_key.encode('utf-8'),
            value=json.dumps(data).encode('utf-8')
        )
        
        if data['stream_type'] == 'bookTicker':
            log.info(f"Published [bookTicker] to Kafka: {partition_key} | Spread: ${data['spread']:.2f}")
        else:
            log.info(f"Published [ticker] to Kafka: {partition_key} | Last: ${data['last_price']:.2f} ({data['pct_change']}%)")
        
    except Exception as e:
        log.error(f"Failed to send message: {e}")

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
