import os

SERVICE_NAMESPACE = os.getenv('SERVICE_NAMESPACE', 'dev')

AEROSPIKE_HOST = os.getenv('AEROSPIKE_HOST', '127.0.0.1')
AEROSPIKE_PORT = int(os.getenv('AEROSPIKE_PORT', 3000))
AEROSPIKE_NAMESPACE = os.getenv('AEROSPIKE_NAMESPACE', 'harpia')

CLIENT_NOTIFICATION_PERIOD_SECONDS = int(os.getenv('CLIENT_NOTIFICATION_PERIOD_SECONDS', 10))

NOTIFICATION_DESTINATION = [
    "ui",
    "email",
    "jira",
    "skype",
    "teams",
    "telegram",
    "pagerduty",
    "sms",
    "voice",
    "whatsapp",
    "signl4",
    "slack",
    "webhook"
]

FORCE_UPDATE = False
