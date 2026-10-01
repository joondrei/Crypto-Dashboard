import json
import websockets

class StreamLoader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.base_url = "wss://stream.binance.com:9443"

    def generate_url(self) -> str:
        """Reads the json config file and compiles the Binance combined stream URL."""
        try:
            with open(self.config_path, 'r') as file:
                config = json.load(file)
            
            self.symbols = config.get("symbols", [])
            self.streams = config.get("streams", [])
            
            if not self.symbols or not self.streams:
                raise ValueError("Config file must contain at least one symbol and one stream type.")
            
            # Combine every symbol with every stream type (e.g., btcusdt@bookTicker)
            combined_streams = []
            for symbol in self.symbols:
                for stream in self.streams:
                    # Binance streams must be lowercase
                    combined_streams.append(f"{symbol.lower()}@{stream}")
            
            # Join all streams with a forward slash
            stream_parameter = "/".join(combined_streams)
            return f"{self.base_url}/stream?streams={stream_parameter}"
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON syntax in configuration file: {self.config_path}")
