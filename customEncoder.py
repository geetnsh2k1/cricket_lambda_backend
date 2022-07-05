import json
from decimal import Decimal
from traceback import print_tb

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        
        return json.JSONEncoder.default(self, obj)