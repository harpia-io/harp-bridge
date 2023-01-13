from microservice_template_core.tools.flask_restplus import api
from flask_restx import Resource
import traceback
from microservice_template_core.tools.logger import get_logger

logger = get_logger()
ns = api.namespace('status', description='Harp Licenses endpoint')


@ns.route('/all')
class LicenseStatus(Resource):
    @staticmethod
    @api.response(200, 'Info has been collected')
    def get():
        """
        Return All exist Licenses
        """

        result = {'licenses': 'new_obj'}

        return result, 200
