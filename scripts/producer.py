import json
import websocket
from datetime import datetime, timezone
import logging
from kafka import KafkaProducer
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.configloader import ConfigLoader
from utils.streamloader import StreamLoader


# ── LOGGING ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

streamconfigfile = PROJECT_ROOT / "configs" / "stream_config.json"
stream_loader = StreamLoader(streamconfigfile)

configfile = PROJECT_ROOT / "configs" / "kafka.env"
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
        stream_name = payload.get('stream', '')
        data = payload.get('data')

        if not data or not stream_name:
            log.error(f"Error empty message")
            return
        stream_type = stream_name.split('@')[1] 
        if stream_type not in stream_loader.streams:
            log.error(f"Error unhandled stream: {stream_name}")
            return


        if stream_type == "ticker":
            record = {
                'symbol': data.get('s', None).upper(),
                'timestamp': int(data.get('E', None)) ,
                'bid_price': float(data.get('b', None)),
                'bid_qty': float(data.get('B', None)),
                'ask_price': float(data.get('a', None)),
                'ask_qty': float(data.get('A', None)),
                # 'spread': float(data.get('c', None)),
                'last_price': float(data.get('c', None)),
                'pct_change': float(data.get('P', None)),
                'avg_price': float(data.get('w', None)),
                'open_price': float(data.get('o', None)),
                'high_price': float(data.get('h', None)),
                'low_price': float(data.get('l', None)),
            }

        if "kline" in stream_type:
            stream_type, interval = stream_type.split('_')
            record = {
                'symbol': data.get('s', None).upper(),
                'interval': interval
                # 'timestamp': int(data.get('E', None)),
            }
            kline = data.get('k', None)
            record["timestamp"] = int(kline.get('t', None))
            # record["close_time"] = kline.get('T', None)
            record["open_price"] = float(kline.get('o', None))
            record["close_price"] = float(kline.get('c', None))
            record["high_price"] = float(kline.get('h', None))
            record["low_price"] = float(kline.get('l', None))
            
        # for (key, value) in data.items():
        #     if value == None:
        #         log.error(f"Error {key} is None")
        #         return
        
        record['timestamp'] = datetime.fromtimestamp(
                            record['timestamp'] / 1000, tz=timezone.utc
                            ).isoformat()

        send_to_kafka(record, stream_type)

    except Exception as e:
        log.error(f"Error merging stream states: {e}")

def on_error(ws, error):
    log.error(f"Error establishing websocket connection: {error}")

def on_close(ws, close_status_code, close_msg):
    log.warning(f"WebSocket closed (status={close_status_code}, msg={close_msg}) - reconnecting")

def on_open(ws):
    log.info(f"Connected to Binance Combined Streams API")

def send_to_kafka(data, stream_type):
    """Send fully filled, zero-null message to Kafka with symbol as partition key"""
    try:
        partition_key = f"{data['symbol']}"
        if stream_type == "kline":
            partition_key = f"{stream_type}:{data['symbol']}:{data['interval']}"
        producer.send(
            topic=stream_type,
            key=partition_key.encode('utf-8'),
            value=json.dumps(data).encode('utf-8')
        )
        
        log.info(f"[{stream_type} Event] Published complete record for {partition_key}")
        
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
    # ping_interval/ping_timeout are the keepalive: without them a connection that
    # goes silent (NAT/bridge timeout, a blip, Binance's 24h session cap) leaves
    # recv() blocked forever — the process stays up, burns no CPU and quietly
    # stops publishing, because neither on_error nor on_close ever fire.
    # reconnect makes run_forever re-establish the stream after any drop.
    ws.run_forever(
        ping_interval=20,
        ping_timeout=10,
        reconnect=5,
    )

    log.info("Flushing remaining Kafka messages...")
    producer.flush(timeout=5)

    log.info("Closing Kafka producer connection...")
    producer.close(timeout=5)
    
    log.info("Shutdown sequence complete. Exiting application safely.")
    sys.exit(0)
