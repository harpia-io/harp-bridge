from microservice_template_core.tools.aerospike_client import AerospikeClient
import harp_bridge.settings as settings
from microservice_template_core.tools.logger import get_logger

logger = get_logger()

aerospike_client_aggr_notifications = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_notifications',
    bin_index={'guid': 'string'}
)

aerospike_client_aggr_statistics = AerospikeClient(
    aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_statistics',
    bin_index={'guid': 'string'}
)


class ForceUpdate(object):
    def __init__(self, alert_ids):
        self.alert_ids = alert_ids

    @staticmethod
    def generate_key(single_alert, single_notification_type):
        aerospike_key = f"{single_alert['studio_id']}___{single_notification_type}___active"

        return aerospike_key

    def get_notification(self, single_alert, single_notification_type):
        aerospike_key = f'{self.generate_key(single_alert, single_notification_type)}___notifications'

        notification = aerospike_client_aggr_notifications.read_message(
            aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_notifications', aerospike_key=aerospike_key
        )

        return notification

    def get_statistics(self, single_alert, single_notification_type):
        aerospike_key = f'{self.generate_key(single_alert, single_notification_type)}___statistics'
        statistics = aerospike_client_aggr_statistics.read_message(
            aerospike_set=f'{settings.SERVICE_NAMESPACE}_aggr_statistics', aerospike_key=aerospike_key
        )

        return statistics

    def update_aerospike(self):
        for single_alert in self.alert_ids:
            for single_notification_type in settings.NOTIFICATION_DESTINATION:
                notification_list = self.get_notification(single_alert, single_notification_type)
                statistics = self.get_statistics(single_alert, single_notification_type)

                for index, notification in enumerate(notification_list['notifications']):
                    if notification['alert_body']['notification_id'] == single_alert['alert_id']:
                        del notification_list['notifications'][index]
                        statistics['statistics'] = statistics['statistics'] - 1

                aerospike_client_aggr_notifications.put_message(
                    f'{settings.SERVICE_NAMESPACE}_aggr_notifications', f'{self.generate_key(single_alert, single_notification_type)}___notifications', notification_list
                )

                aerospike_client_aggr_statistics.put_message(
                    f'{settings.SERVICE_NAMESPACE}_aggr_statistics', f'{self.generate_key(single_alert, single_notification_type)}___statistics', statistics
                )

    def main(self):
        self.update_aerospike()
        settings.FORCE_UPDATE = True

