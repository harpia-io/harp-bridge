notifications = {"severity": 3, "source": "API Source0"}

print(' AND '.join("{!s}={!r}".format(key,val) for (key,val) in notifications.items()))