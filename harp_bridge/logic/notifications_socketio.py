import json
from threading import Lock
from flask import request
from flask_socketio import Namespace, emit, join_room, leave_room, close_room, rooms, disconnect
from harp_bridge.logic.notification_extractor import CreateRoom
from microservice_template_core.tools.logger import get_logger
import harp_bridge.settings as settings
from microservice_template_core.settings import ServiceConfig
import traceback
from harp_bridge.metrics.service_monitoring import Prom
from itertools import groupby
from operator import itemgetter
import re

logger = get_logger()

thread = None
thread_force_update = None

thread_lock = Lock()
ROOMS = {}


class NotificationsNamespace(Namespace):
    def __init__(self, namespace=None):
        super(Namespace, self).__init__(namespace)
        self.socketio = None

    def on_my_event(self, message):
        emit('my_response', {'data': message['data']})

    def on_my_broadcast_event(self, message):
        emit('my_response', {'data': message['data']}, broadcast=True)

    def on_join(self, message):
        room = request.sid
        if len(room) >= 1:
            join_room(room)
            logger.info(msg=f'Created new room - {room}. SID - {request.sid}')
            emit('my_response', {'data': 'In rooms: ' + ', '.join(rooms())})
        else:
            logger.warn(msg=f'Room name is empty - {message}')
            emit('my_response', {'data': f'Client can`t connect to room since name is empty - {message}'})

    def on_leave(self, message):
        room = request.sid
        try:
            del ROOMS[request.sid]
        except KeyError:
            logger.warn(msg=f"No room {request.sid} to delete from broadcast")
        leave_room(room)
        logger.info(msg=f"Deleted room - {room}. Reason - client left it. SID - {request.sid}")
        emit('my_response', {'data': 'In rooms: ' + ', '.join(rooms())})

    def on_close_room(self, message):
        room = request.sid
        try:
            del ROOMS[request.sid]
        except KeyError:
            logger.warn(msg=f"No room {request.sid} to delete from broadcast")
        logger.info(msg=f"Deleted room - {room}. Reason - client close it. SID - {request.sid}")
        emit('my_response', {'data': 'Room ' + room + ' is closing.'}, room=message['room'])
        close_room(room)

    @Prom.CLIENT_UPDATE_FILTER.time()
    def on_update_filter(self, message):
        logger.info(msg=f"Received request to update client filter - {request.sid}. Message: {message}")

        try:
            user_name = message['user_name']
            user_data = message['user_data']
            user_sid = request.sid

            if len(user_name) >= 1 and len(user_data) >= 1:
                user_data['user_name'] = user_name
                ROOMS[user_sid] = user_data
                self.update_room(room_name=user_sid, room_metadata=user_data)
                logger.info(msg=f'Activated notifications broadcast for user - {user_name}.\nInput filter: {json.dumps(user_data)}\nSID - {user_sid}')
            else:
                logger.warn(msg=f'User name or user filter is empty - {message}')
                emit('my_response', {'data': f'Client can`t subscribe to events since user name or user filter is empty - {message}'})

            Prom.NOTIFICATIONS_UPDATE_FILTER.labels(user_name=user_name).inc(1)
            emit('my_response', {'data': 'Filter was updated'})
        except Exception as err:
            logger.error(msg=f"Can`t update filter for client. Message - {message}\nError - {err}\nTraceback: {traceback.format_exc()}")
            emit('my_response', {'data': 'Can`t update filter for client. Please check backend logs for more details'})

    @Prom.CLIENT_DISCONNECT.time()
    def on_disconnect_request(self):
        try:
            del ROOMS[request.sid]
        except KeyError:
            logger.warn(msg=f"No room {request.sid} to delete from broadcast")
        logger.info(msg=f"Client sent request to be disconnected. SID - {request.sid}")
        emit('my_response', {'data': 'Disconnected!'})
        disconnect()

    def on_my_ping(self):
        emit('my_pong')

    @Prom.CLIENT_CONNECT.time()
    def on_connect(self):
        user_sid = request.sid
        logger.info(msg=f"Received request to connect new client - {user_sid}.\n")
        join_room(user_sid)
        logger.info(msg=f'Created new room - {user_sid}')
        self.run_thread()
        logger.info(msg=f"Client was connected to room - {user_sid}")

        emit('my_response', {'data': f'Connected to room - {user_sid}'})

    @Prom.CLIENT_DISCONNECT.time()
    def on_disconnect(self):
        try:
            del ROOMS[request.sid]
        except KeyError:
            logger.warn(msg=f"No room {request.sid} to delete from broadcast")
        logger.info(msg=f'Client disconnected - {request.sid}')
        emit('my_response', {'data': 'Disconnected!'})
        disconnect()

    @staticmethod
    @Prom.FILTER_NOTIFICATIONS.time()
    def filter_notifications_by_labels(notifications, room_metadata):
        notifications_to_show = []
        for single_notifications in notifications:
            main_fields = {
                'monitoring_system': single_notifications['alert_body']['body']['monitoring_system'],
                'source': single_notifications['alert_body']['body']['source'],
                'alert_name': single_notifications['alert_body']['body']['name']
            }
            additional_fields = {**single_notifications['alert_body']['body']['additional_fields'], **main_fields}
            notification_candidate = []
            for tag_filter in room_metadata['tagFilter']:
                if tag_filter['tag'] in additional_fields:
                    if tag_filter['condition'] == '=':
                        if tag_filter['value'] == additional_fields[tag_filter['tag']]:
                            notification_candidate.append(single_notifications)
                    elif tag_filter['condition'] == '!=':
                        if tag_filter['value'] != additional_fields[tag_filter['tag']]:
                            notification_candidate.append(single_notifications)
                    elif tag_filter['condition'] == '~':
                        if bool(re.match(rf"{tag_filter['value']}", additional_fields[tag_filter['tag']])) is True:
                            notification_candidate.append(single_notifications)
                    elif tag_filter['condition'] == '!~':
                        if bool(re.match(rf"{tag_filter['value']}", additional_fields[tag_filter['tag']])) is False:
                            notification_candidate.append(single_notifications)

            # if len(list({v['tag']: v for v in room_metadata['tagFilter']}.values())) == len(notification_candidate):
            #     notifications_to_show.append(single_notifications)

            if len(room_metadata['tagFilter']) == len(notification_candidate):
                notifications_to_show.append(single_notifications)

        return notifications_to_show

    @staticmethod
    def filter_notification_by_assign(notifications, username):
        notifications_to_show = []
        for single_notifications in notifications:
            assigned_to = single_notifications['alert_body']['assigned_to']
            if 'username' in assigned_to:
                if assigned_to['username'] == username:
                    notifications_to_show.append(single_notifications)

        return notifications_to_show

    @staticmethod
    @Prom.GROUP_NOTIFICATIONS.time()
    def group_notifications(notifications, room_metadata):
        notifications_without_group_key = []
        notifications_with_group_key = []
        notification_groups = []

        # Remove notifications without group tags
        for key, value in enumerate(notifications):
            tag_exist = value.keys() >= set(room_metadata['tagGrouping'])
            if tag_exist is False:
                value['alert_body']['panel_type'] = 'single_alert'
                notifications_without_group_key.append(value['alert_body'])
            else:
                notifications_with_group_key.append(value)

        notifications_with_group_key.sort(key=itemgetter(*room_metadata['tagGrouping']))

        groups = groupby(notifications_with_group_key, key=itemgetter(*room_metadata['tagGrouping']))

        for tag_grouping, group in groups:
            if isinstance(tag_grouping, tuple):
                dict_tag_grouping = dict(zip(room_metadata['tagGrouping'], list(tag_grouping)))
            else:
                dict_tag_grouping = dict(zip(room_metadata['tagGrouping'], [str(tag_grouping)]))

            format_tag_grouping = {
                "Group By": ' AND '.join("{!s}={!r}".format(key, val) for (key, val) in dict_tag_grouping.items())}

            grouping = {
                "group_tags": format_tag_grouping,
                "group_notifications": [],
                "group_notifications_count": 0,
                "last_change_ts": None,
                "current_duration": 0,
                "panel_type": "grouping_alert"
            }
            for content in group:
                grouping['group_notifications'].append(content['alert_body'])

            if len(grouping['group_notifications']) == 1:
                grouping['group_notifications'][0]['panel_type'] = 'single_alert'
                notifications_without_group_key.append(grouping['group_notifications'][0])
                continue

            if room_metadata['notificationSorting']['sort_type'] == 'sorted':
                grouping['group_notifications'].sort(key=itemgetter('current_duration'))
            elif room_metadata['notificationSorting']['sort_type'] == 'reverse':
                grouping['group_notifications'].sort(key=itemgetter('current_duration'), reverse=True)

            grouping['current_duration'] = grouping['group_notifications'][0]['current_duration']
            grouping['group_notifications_count'] = len(grouping['group_notifications'])

            notification_groups.append(grouping)

        notifications_total = notification_groups + notifications_without_group_key

        return notifications_total

    @staticmethod
    def sort_notifications(notifications, room_metadata):
        if room_metadata['notificationSorting']['sort_type'] == 'sorted':
            notifications.sort(key=itemgetter('current_duration'))
        elif room_metadata['notificationSorting']['sort_type'] == 'reverse':
            notifications.sort(key=itemgetter('current_duration'), reverse=True)

        return notifications

    @Prom.CLIENT_UPDATE_ROOM.time()
    def update_room(self, room_name, room_metadata):
        Prom.NOTIFICATIONS_BROADCAST.labels(user_name=room_metadata['user_name']).inc(1)

        create_room = CreateRoom(room_metadata)
        notifications, statistics = create_room.read_from_aerospike()

        logger.info(msg=f"update_room: room_metadata: {json.dumps(room_metadata)}\nnotifications: {json.dumps(notifications)}")

        if 'showAssignedToMe' in room_metadata:
            if room_metadata['showAssignedToMe']:
                try:
                    notifications = self.filter_notification_by_assign(notifications=notifications, username=room_metadata['user_name'])
                except Exception as error:
                    logger.error(msg=f"Can`t filter by assign - {error}\nTrace: {traceback.format_exc()}")

        if len(room_metadata['tagFilter']) > 0:
            try:
                notifications = self.filter_notifications_by_labels(notifications, room_metadata)
            except Exception as error:
                logger.error(msg=f"Can`t filter labels - {error}\nTrace: {traceback.format_exc()}")

        if len(room_metadata['tagGrouping']) > 0:
            try:
                notifications = self.group_notifications(notifications, room_metadata)
                logger.info(msg=f"tagGrouping True: room_metadata: {json.dumps(room_metadata)}\nnotifications: {json.dumps(notifications)}")
            except Exception as error:
                logger.error(msg=f"Can`t group notifications - {error}\nTrace: {traceback.format_exc()}")

        else:
            decorated_notifications = []
            for single_notification in notifications:
                decorated_notifications.append(single_notification['alert_body'])

            notifications = decorated_notifications
            logger.info(msg=f"tagGrouping False: room_metadata: {json.dumps(room_metadata)}\nnotifications: {json.dumps(notifications)}")

        notifications = self.sort_notifications(notifications, room_metadata)

        self.socketio.emit(
            'notifications',
            {
                'data': notifications
            },
            namespace=f'/{ServiceConfig.SERVICE_NAME}/notifications',
            room=room_name
        )

        self.socketio.emit(
            'statistics',
            {
                'data': statistics
            },
            namespace=f'/{ServiceConfig.SERVICE_NAME}/notifications',
            room=room_name
        )

    def run_thread(self):
        global thread
        global thread_force_update

        with thread_lock:
            if thread is None:
                thread = self.socketio.start_background_task(
                    self.background_thread
                )
                logger.info(msg=f"Start new background thread - {thread}")
            else:
                logger.info(msg=f"Background thread was already started - {thread}")

            if thread_force_update is None:
                thread_force_update = self.socketio.start_background_task(
                    self.force_update
                )
                logger.info(msg=f"Start new background thread - {thread_force_update}")
            else:
                logger.info(msg=f"Background thread was already started - {thread_force_update}")

    def background_thread(self):
        while True:
            try:
                # logger.info(msg=f'Current list of rooms - {ROOMS}')
                self.socketio.sleep(settings.CLIENT_NOTIFICATION_PERIOD_SECONDS)
                for room_name, room_metadata in list(ROOMS.items()):
                    self.update_room(room_name=room_name, room_metadata=room_metadata)
            except Exception as err:
                logger.error(msg=f"Can`t update user rooms - {ROOMS}\nError: {err}\nTrace: {traceback.format_exc()}")

    def force_update(self):
        while True:
            try:
                self.socketio.sleep(1)
                if settings.FORCE_UPDATE:
                    for room_name, room_metadata in ROOMS.items():
                        self.update_room(room_name=room_name, room_metadata=room_metadata)
                    settings.FORCE_UPDATE = False
            except Exception as err:
                logger.error(msg=f"Can`t force update user rooms - {ROOMS}\nError: {err}\nTrace: {traceback.format_exc()}")