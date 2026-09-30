import os

class ConfigLoader:
    """Loads, stores, and exposes configuration fields for a Kafka Producer."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._config = {}
        self._load_from_file()
        
    def _load_from_file(self):
        """Reads the raw file and stores properties internally."""
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Configuration file not found at: {self.filepath}")
            
        with open(self.filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                key, val = line.split('=', 1)
                self._config[key.strip()] = val.strip()

    # --- Type-Safe Getter Methods ---

    def get_bootstrap_servers(self) -> str:
        """Returns the Kafka broker addresses."""
        return self._config.get("BOOTSTRAP_SERVERS", "localhost:9092")

    def get_client_id(self) -> str:
        """Returns the client identifier."""
        return self._config.get("CLIENT_ID", "python-producer")

    # def get_security_protocol(self) -> str:
    #     """Returns the security protocol (e.g., PLAINTEXT, SASL_SSL)."""
    #     return self._config.get("SECURITY_PROTOCOL", "PLAINTEXT")

    def get_sasl_mechanism(self) -> str:
        """Returns the SASL authentication mechanism."""
        return self._config.get("SASL_MECHANISMS", "PLAIN")

    def get_sasl_username(self) -> str | None:
        """Returns the SASL username or None if not set."""
        return self._config.get("SASL_USERNAME")

    def get_sasl_password(self) -> str | None:
        """Returns the SASL password or None if not set."""
        return self._config.get("SASL_PASSWORD")

    def get_acks(self) -> str:
        """Returns the acknowledgment strategy (e.g., 'all', '1', '0')."""
        return self._config.get("ACKS", "all")

    def get_linger_ms(self) -> int:
        """Returns message production delay buffer in milliseconds."""
        return int(self._config.get("LINGER_MS", 20))

    def get_retries(self) -> int:
        """Returns the maximum number of retry attempts."""
        return int(self._config.get("RETRIES", 5))

    def to_kafka_dict(self) -> dict:
        """Assembles and maps fields to official Kafka driver names."""
        kafka_map = {
            "bootstrap_servers": self.get_bootstrap_servers(),
            "client_id": self.get_client_id(),
            # "security_protocol": self.get_security_protocol(),
            "acks": self.get_acks(),
            "linger_ms": self.get_linger_ms(),
            "retries": self.get_retries()
        }
        
        # # Only inject SASL parameters if authenticated protocol is chosen
        # if "SASL" in self.get_security_protocol():
        #     kafka_map["sasl.mechanism"] = self.get_sasl_mechanism()
        #     kafka_map["sasl.username"] = self.get_sasl_username()
        #     kafka_map["sasl.password"] = self.get_sasl_password()
            
        return kafka_map