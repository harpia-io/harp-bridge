from microservice_template_core.tools.logger import get_logger
from microservice_template_core.tools.aerospike_client import AerospikeClient
import harp_bridge.settings as settings

logger = get_logger()


aerospike_client_environments = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_environments',
    bin_index={'guid': 'string'}
)

aerospike_client_aggr_notifications = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_notifications',
    bin_index={'guid': 'string'}
)

aerospike_client_aggr_statistics = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_statistics',
    bin_index={'guid': 'string'}
)


class CreateRoom(object):
    def __init__(self, user_filter):
        self.user_filter = user_filter

    def filter_data(self):
        rooms = []
        try:
            for env in self.user_filter['environments']:
                for dest in self.user_filter['destinationFilter']:
                    for state in self.user_filter['notificationStatesFilter']:
                        rooms.append(f"{env}___{dest}___{state}")
        except Exception as err:
            logger.error(
                msg=f"Can`t prepare full list of Aerospike keys for rooms based on user input - {self.user_filter}\nERROR: {err}"
            )
            return rooms

        return rooms

    def prepare_notifications(self):
        all_notifications = []
        notification_statistics = {
            'active': 0,
            'snoozed': 0,
            'acknowledged': 0,
            'in_downtime': 0,
            'assigned': 0,
            'handled': 0
        }
        env_statistics = aerospike_client_environments.read_message(aerospike_set=f'{settings.SERVICE_NAMESPACE}_environments', aerospike_key=f'statistics')

        for room in self.filter_data():
            notifications_in_room = aerospike_client_aggr_notifications.read_message(aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_notifications', aerospike_key=f'{room}___notifications')
            statistics_in_room = aerospike_client_aggr_statistics.read_message(aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_statistics', aerospike_key=f'{room}___statistics')

            if notifications_in_room:
                all_notifications = all_notifications + notifications_in_room['notifications']

            if statistics_in_room:
                notification_state = room.split('___')[-1]
                if notification_state in notification_statistics:
                    notification_statistics[notification_state] += statistics_in_room['statistics']
                else:
                    notification_statistics[notification_state] = statistics_in_room['statistics']

        statistics = {
            'environments': env_statistics,
            'filtered_states': notification_statistics
        }

        return all_notifications, statistics

    def read_from_aerospike(self):
        notifications, statistics = self.prepare_notifications()

        return notifications, statistics
