import json
import websocket
from datetime import datetime, timezone
import logging
from kafka import KafkaProducer
import signal
import sys
from configloader import ConfigLoader


# ── LOGGING ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)
topic_name = "crypto.livestream"

# Base Binance WebSocket URL
BINANCE_WS_URL = "wss://stream.binance.com:9443"

# List your streams here (symbols MUST be lowercase)
STREAMS = [
    "btcusdt@trade",      # Bitcoin trade stream
    "ethusdt@trade",      # Ethereum trade stream
    "solusdt@kline_1m"    # Solana 1-minute candlesticks
]

configfile = "kafka.env"
config = ConfigLoader(configfile)
producer_config = config.to_kafka_dict()

producer = KafkaProducer(
    **producer_config   
)  

# Join the streams with forward slashes for the combined endpoint URL
COMBINED_URL = f"{BINANCE_WS_URL}/stream?streams={'/'.join(STREAMS)}"

ws = None

def handle_shutdown(signum, frame):
    """Callback function triggered when Ctrl+C or terminal termination is requested"""
    log.info(f"Received signal {signum}. Commencing graceful shutdown...")
    if ws:
        log.info("Closing WebSocket connection...")
        ws.close()  # This breaks the ws.run_forever() block below


def on_message(ws, message):

    try:
            
        payload = json.loads(message)
        
        # Combined streams wrap the raw event data inside an outer dict containing 'stream' and 'data' keys
        stream_name = payload.get('stream')
        data = payload.get('data')

        symbol = data.get('s',"")
        price = data.get('p',None)
        qty = data.get('q',None)
        timestamp = data.get('T',None)

        if any(v is None for v in (symbol, price, qty, timestamp)):
            return
        try:
            price = float(price)
        except Exception as e:
            log.error(f"Error converting price string {price} to float: {e}")

        try:
            qty = float(qty)
        except Exception as e:
            log.error(f"Error converting qty string {qty} to float: {e}")

        try:
            timestamp = datetime.fromtimestamp(
                timestamp / 1000, tz=timezone.utc
            ).isoformat()
        except Exception as e:
            log.error(f"Error converting timestamp {timestamp} to datetime: {e}")

        trade_data = {
            'symbol': symbol,
            'price': price,
            'quantity': qty,
            'tradeTime': timestamp,
        }

        send_to_kafka(trade_data)

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
        # The partition key determines which partition this goes to
        partition_key = data['symbol']  # "BTC-USD", "ETH-USD", or "SOL-USD"
        
        producer.send(
            topic=topic_name,
            key=partition_key.encode('utf-8'),  # Convert to bytes
            value=json.dumps(data).encode('utf-8')  # Serialize to JSON bytes
        )
        
        log.info(f"Message sent to kafka: {partition_key} @ ${data['price']}")
        
    except Exception as e:
        log.error(f"Failed to send message: {e}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    ws = websocket.WebSocketApp(
        COMBINED_URL,
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
