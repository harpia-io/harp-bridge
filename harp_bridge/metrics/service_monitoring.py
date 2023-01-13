from microservice_template_core.tools.prometheus_metrics import Gauge, Counter, Summary, Histogram


class Prom:
    CLIENT_UPDATE_ROOM = Summary('client_update_room_latency_seconds', 'Time spent processing client update room')
    CLIENT_UPDATE_FILTER = Summary('client_update_filter_latency_seconds', 'Time spent processing client update filter')
    CLIENT_DISCONNECT = Summary('client_disconnect_latency_seconds', 'Time spent processing client disconnect')
    CLIENT_CONNECT = Summary('client_connect_latency_seconds', 'Time spent processing client connect')
    GROUP_NOTIFICATIONS = Summary('group_notifications_latency_seconds', 'Time spent processing group notifications')
    FILTER_NOTIFICATIONS = Summary('filter_notifications_latency_seconds', 'Time spent processing filter notifications')
    NOTIFICATIONS_BROADCAST = Counter('notification_broadcast', 'notification_broadcast', [
        'user_name'
    ])
    NOTIFICATIONS_UPDATE_FILTER = Counter('notification_update_filter', 'notification_update_filter', [
        'user_name'
    ])
