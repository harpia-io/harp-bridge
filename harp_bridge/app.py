from microservice_template_core.tools.logger import get_logger
from flask_socketio import SocketIO
from flask import Flask, render_template
from harp_bridge.logic.notifications_socketio import NotificationsNamespace
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from microservice_template_core.settings import ServiceConfig
from flask import request
from harp_bridge.logic.force_update import ForceUpdate
import traceback

logger = get_logger()

async_mode = None
app = Flask(__name__)
PrometheusMetrics(app)
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {'/metrics': make_wsgi_app()})

service_socketio = SocketIO(
    app,
    async_mode=async_mode,
    logger=True,
    cors_allowed_origins="*",
    engineio_logger=True,
    path=f"/{ServiceConfig.SERVICE_NAME}/socket.io"
)


@app.route(f'/{ServiceConfig.SERVICE_NAME}')
def index():
    return render_template('index.html', async_mode=service_socketio.async_mode)


@app.route(f'/{ServiceConfig.SERVICE_NAME}/health')
def health():
    return {"msg": "Healthy"}, 200


@app.route(f'/{ServiceConfig.SERVICE_NAME}/api/v1/bridge/force_update', methods=['POST'])
def force_update():
    """
    Endpoint to run force update after action - snooze, ack, handle etc..
    * Send a JSON object
    ```
    [
        {
            "alert_id": 123,
            "studio_id": 101,
            "notification_type": 5
        },
        {
            "alert_id": 124,
            "studio_id": 101,
            "notification_type": 5
        }
    ]
    ```
    """
    try:
        data = request.get_json()
        logger.info(f'Received request to force update - {data}')
        execute_force_update = ForceUpdate(alert_ids=data)
        execute_force_update.main()
    except Exception as err:
        logger.error(msg=f"Can`t force update. Error - {err}. Trace: {traceback.format_exc()}")
        return {"msg": f"Can`t force update. Error - {err}"}, 500

    return {"status": "Force update was completed"}, 200


def init_socketio():
    logger.info(msg=f"Registering namespace")
    service_socketio.on_namespace(NotificationsNamespace(f'/{ServiceConfig.SERVICE_NAME}/notifications'))
    logger.info(msg=f"Starting Flask")
    service_socketio.run(app, port=8081, debug=False, host="0.0.0.0")


def main():
    init_socketio()
    logger.debug(msg=f"Flask has been started")


if __name__ == '__main__':
    main()

