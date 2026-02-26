import os
import json
import uuid
import random
import time
from datetime import datetime, timezone  # Added timezone

from faker import Faker
from faker.config import AVAILABLE_LOCALES
import pycountry

from azure.eventhub import EventHubProducerClient, EventData

# -----------------------------
# EVENT HUB CONFIG
# -----------------------------
# Ensure these environment variables are set in your local terminal/env
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

if not EVENT_HUB_CONN_STR or not EVENT_HUB_NAME:
    raise ValueError("❌ Event Hub connection string or name not set")

# -----------------------------
# SAFE LOCALE HANDLING
# -----------------------------
SUPPORTED_LOCALES = set(AVAILABLE_LOCALES)

COUNTRY_LOCALE_MAP = {
    "US": "en_US", "CA": "en_CA", "MX": "es_MX",
    "BR": "pt_BR", "AR": "es_AR", "CL": "es_CL",
    "CO": "es_CO", "PE": "es_PE", "VE": "es_VE",
    "GB": "en_GB", "FR": "fr_FR", "DE": "de_DE",
    "ES": "es_ES", "IT": "it_IT", "NL": "nl_NL",
    "SE": "sv_SE", "NO": "no_NO", "DK": "da_DK",
    "FI": "fi_FI", "PL": "pl_PL", "CZ": "cs_CZ",
    "HU": "hu_HU", "RO": "ro_RO", "PT": "pt_PT",
    "GR": "el_GR", "UA": "uk_UA", "RU": "ru_RU",
    "AE": "ar_AE", "SA": "ar_SA", "IL": "he_IL",
    "TR": "tr_TR", "IN": "en_IN", "PK": "en_GB",
    "BD": "en_GB", "CN": "zh_CN", "JP": "ja_JP",
    "KR": "ko_KR", "SG": "en_SG", "MY": "ms_MY",
    "TH": "th_TH", "VN": "vi_VN", "ID": "id_ID",
    "PH": "en_GB", "AU": "en_AU", "NZ": "en_NZ",
    "ZA": "en_ZA", "NG": "en_GB", "KE": "en_GB",
    "EG": "ar_EG", "MA": "fr_FR"
}

DEFAULT_LOCALE = "en_US"

# Pre-caching fakers to improve performance
faker_cache = {}

def get_faker(locale: str) -> Faker:
    if locale not in faker_cache:
        safe_loc = locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE
        faker_cache[locale] = Faker(safe_loc)
    return faker_cache[locale]

# -----------------------------
# DATA DIMENSIONS
# -----------------------------
USERS = [f"user_{i}" for i in range(1, 100_001)]
COUNTRIES = [c.alpha_2 for c in list(pycountry.countries)[:100]]
MERCHANT_CATEGORIES = ["ecommerce", "travel", "food", "subscription", "electronics", "gaming", "fintech", "health"]
MERCHANTS = [f"{cat}_merchant_{i}" for cat in MERCHANT_CATEGORIES for i in range(1, 26)][:200]
DEVICE_TYPES = ["mobile", "web", "pos"]

# -----------------------------
# TRANSACTION GENERATOR
# -----------------------------
def generate_transaction():
    user_id = random.choice(USERS)
    merchant = random.choice(MERCHANTS)
    country_code = random.choice(COUNTRIES)

    locale = COUNTRY_LOCALE_MAP.get(country_code, DEFAULT_LOCALE)
    faker = get_faker(locale)

    amount = round(random.uniform(5, 500), 2)
    is_fraud = False
    risk_reason = "normal"

    # Simulate fraud patterns
    if random.random() < 0.06:
        is_fraud = True
        risk_reason = random.choice(["high_amount", "velocity_attack", "geo_anomaly", "risky_merchant"])
        amount *= random.randint(5, 12)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "merchant_id": merchant,
        "amount": round(amount, 2),
        "currency": "USD",
        "country_code": country_code,
        "city": faker.city(),
        "device_type": random.choice(DEVICE_TYPES),
        # FIX: Using aware datetime objects to ensure Spark watermarks align with real-world time
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        "is_fraud": is_fraud,
        "risk_reason": risk_reason
    }

# -----------------------------
# EVENT HUB STREAMING
# -----------------------------
def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    print("🚀 Streaming transactions to Azure Event Hub...")

    try:
        while True:
            batch = producer.create_batch()
            
            # Send small, frequent batches to move the Spark watermark forward
            for _ in range(random.randint(5, 15)):
                txn = generate_transaction()
                batch.add(EventData(json.dumps(txn)))

            producer.send_batch(batch)
            
            # Print current UTC time for easy verification with Spark stats
            curr_utc = datetime.now(timezone.utc).strftime('%H:%M:%S')
            print(f"✅ Batch sent at {curr_utc} UTC")

            # Shorter sleep to keep the pipeline "hot"
            time.sleep(random.uniform(0.5, 1.0))

    except KeyboardInterrupt:
        print("\n⛔ Streaming stopped by user")
    finally:
        producer.close()

if __name__ == "__main__":
    main()