from gevent import monkey
from gevent.wsgi import WSGIServer
monkey.patch_all()
import os
import requests
from datetime import datetime, timedelta
import redis
import traceback
import math
import time
import json
import jwt
from flask import Flask, Response, request, jsonify, current_app, redirect, url_for, session
from flask_cors import CORS
from flask_oauthlib.client import OAuth
from pymongo import MongoClient, DESCENDING
from bson import ObjectId
from functools import wraps

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# Configuration
etf_host = 'localhost'
redis_host = 'localhost'
redirect_host = os.environ.get('ETF_API_OAUTH_REDIRECT', 'http://localhost:3000')

mongo_client = MongoClient()

mongo_db = mongo_client.eveexchange

settings_collection = mongo_db.settings
portfolio_collection = mongo_db.portfolios
subscription_collection = mongo_db.subscription
users_collection = mongo_db.users
notification_collection = mongo_db.notifications

portfolio_limit = 10 # Max number of portfolios a user can have

port = int(os.environ.get('ETF_API_PORT', 5000))
env = os.environ.get('ETF_API_ENV', 'development')

debug = False if env == 'production' else True

auth_jwt_secret = 'development' if debug else os.environ.get('ETF_API_JWT_SECRET', 'production')

# Use to connect to private backend publishing API
admin_secret = os.environ.get('ETF_API_ADMIN_SECRET', 'admin_secret')

# Application
app = Flask(__name__)
app.secret_key = os.environ.get('ETF_API_JWT_SECRET', 'production')
CORS(app)
oauth = OAuth(app)
evesso = oauth.remote_app('evesso',
    consumer_key=os.environ.get('ETF_API_OAUTH_KEY', 'example'),
    consumer_secret=os.environ.get('ETF_API_OAUTH_SECRET', 'example'),
    request_token_params={'scope': ''},
    base_url='https://login.eveonline.com/',
    request_token_url=None,
    access_token_method='POST',
    access_token_url='https://login.eveonline.com/oauth/token',
    authorize_url='https://login.eveonline.com/oauth/authorize'
)

blueprints = []
blueprints_json = {}

market_ids = []
market_groups = []
market_groups_json = []

with open('sde/blueprints.js', 'r', encoding='utf-8') as f:
    read_data = f.read()
    blueprints_json = read_data
    blueprints = json.loads(read_data)

with open('sde/market_groups.js', 'r', encoding='utf-8') as f:
    read_data = f.read()
    market_groups_json = read_data
    market_groups = json.loads(read_data)

    _getItems = lambda items: [x['id'] for x in items]

    def _getGroups(group, ids):
        if 'items' in group:
            ids.extend(_getItems(group['items']))
        for _group in group['childGroups']:
            _getGroups(_group, ids)

    for group in market_groups:
        _getGroups(group, market_ids)

re = None

try:
    re = redis.StrictRedis(host=redis_host, port=6379, db=0)
except:
    print("Redis server is unavailable")

# Decorator to validate a JWT and retrieve the users info from rethinkDB
# Authorization types:
#   Token <jwt>
#   Key <api_key>
def verify_jwt(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):

        start = time.perf_counter()

        res = None
        user_settings = None

        if request.is_json == False:
            return jsonify({ 'error': "Request must be in json format", 'code': 400 })

        try:
            auth_header = request.headers.get('Authorization')

            if auth_header == None:
                auth_header = request.headers.get('authorization')

            if auth_header == None:
                return jsonify({ 'error': "Authorization header is missing", 'code': 400 })

            split = auth_header.split(" ")

            if len(split) != 2:
                return jsonify({ 'error': "Invalid authorization header format", 'code': 400 })

            if split[0] != "Token" and split[1] != "Key":
                return jsonify({ 'error': "Authorization header must include 'Token' or 'Key'", 'code': 400 })

            if split[0] == "Token":
                try:
                    user_data = jwt.decode(split[1], auth_jwt_secret)

                    user_settings = mongo_db.settings.find_one({'user_id': user_data['user_id']})
                except jwt.exceptions.ExpiredSignatureError:
                    return jsonify({'error': "Authorization token is expired", 'code': 400})
                except jwt.exceptions.InvalidTokenError:
                    return jsonify({'error': "Authorization token is invalid", 'code': 400})
                except:
                    return jsonify({'error': "Failed to parse authorization token", 'code': 400})
        except:
            traceback.print_exc()
            return jsonify({ 'error': "Failed to parse authorization header", 'code': 400 })

        if user_settings is None:
            return jsonify({'error': "Failed to look up user information", 'code': 400})

        user_id = user_settings['user_id']

        return fn(user_id=user_id, settings=user_settings, *args, **kwargs)

    return wrapper

# Routes
@app.route('/', methods=['GET'])
def index():
    return current_app.send_static_file('api.html')

@app.route('/market/forecast/', methods=['GET'])
@verify_jwt
def forecast(user_id, settings):

    # Validation
    try:
        minspread = request.args.get('minspread')
        maxspread = request.args.get('maxspread')
        minvolume = request.args.get('minvolume')
        maxvolume = request.args.get('maxvolume')
        minprice = request.args.get('minprice')
        maxprice = request.args.get('maxprice')
    except:
        return jsonify({ 'error': "Invalid type used in query parameters", 'code': 400 })

    if minspread == None and maxspread == None:
        return jsonify({ 'error': "At least one of minspread and maxspread must be provided", 'code': 400 })

    if minvolume == None and maxvolume == None:
        return jsonify({ 'error': "At least one of minvolume and maxvolume must be provided", 'code': 400 })

    if minprice == None and maxprice == None:
        return jsonify({ 'error': "At least one of minprice and maxprice must be provided", 'code': 400 })

    # Further validation and default values
    try:
        if minspread:
            minspread = float(minspread)
        else:
            minspread = 0
        if maxspread:
            maxspread = float(maxspread)
        else:
            maxspread = 100
        if maxvolume:
            maxvolume = float(maxvolume)
        else:
            maxvolume = 1000000000000
        if minvolume:
            minvolume = float(minvolume)
        else:
            minvolume = 0
        if maxprice:
            maxprice = float(maxprice)
        else:
            maxprice = 1000000000000
        if minprice:
            minprice = float(minprice)
        else:
            minprice = 0
    except:
        return jsonify({ 'error': "One of the provided parameters are not a floating point or integer type.", 'code': 400 })

    # Normalize the values
    '''
    if minspread > maxspread:
        minspread = maxspread - 1
    if maxspread > minspread:
        maxspread = minspread + 1
    '''

    # Load data from redis cache
    allkeys = []
    idx = 0
    first = True

    while idx != 0 or first == True:
        keys = re.scan(match='dly:*', cursor=idx)
        idx = keys[0]
        allkeys.extend(keys[1])
        first = False

    pip = re.pipeline()

    for k in allkeys:
        pip.hmget(k, ['type', 'spreadSMA', 'tradeVolumeSMA', 'buyFifthPercentile'])

    docs = pip.execute()

    # Find ideal matches to query params
    ideal = [doc[0] for doc in docs if doc[1] is not None and doc[2] is not None and doc[3] is not None and float(doc[1]) >= minspread and float(doc[1]) <= maxspread and float(doc[2]) >= minvolume and float(doc[2]) <= maxvolume and float(doc[3]) >= minprice and float(doc[3]) <= maxprice ]

    # Pull out complete documents for all ideal matches

    pip = re.pipeline()

    for k in ideal:
        pip.hgetall('dly:'+k.decode('ascii'))

    # Execute and grab only wanted attributes
    docs = [{key.decode('ascii'):float(row[key]) for key in (b'type', b'spread', b'tradeVolume', b'buyFifthPercentile', b'spreadSMA', b'tradeVolumeSMA', b'sellFifthPercentile')} for row in pip.execute()]

    return jsonify(docs)

@app.route('/market/current/<int:typeid>', methods=['GET'])
@verify_jwt
def market_current(typeid, user_id, settings):

    if isinstance(typeid, int) == False:
        return jsonify({ 'error': "Required parameter 'typeID' is not a valid integer", 'code': 400 })

    if re.exists('cur:'+str(typeid)) == False:
        return jsonify({ 'error': "Failed to find current market data for the given typeID", 'code': 400 })

    reDoc = re.hgetall('cur:'+str(typeid))

    return jsonify({key.decode('ascii'):float(reDoc[key]) for key in (b'type', b'spread', b'tradeVolume', b'buyFifthPercentile', b'sellFifthPercentile')})

@app.route('/portfolio/create', methods=['POST'])
@verify_jwt
def create_portfolio(user_id, settings):

    if request.is_json == False:
        return jsonify({ 'error': "Request must be in json format", 'code': 400 })

    try:
        if request.json is None:
            return jsonify({ 'error': "Request must be in json format", 'code': 400 })

    except:
        return jsonify({ 'error': "There was a problem parsing your json request", 'code': 400 })

    if 'name' not in request.json:
        return jsonify({ 'error': "Required parameter 'name' is missing", 'code': 400 })
    if 'description' not in request.json:
        return jsonify({ 'error': "Required parameter 'description' is missing", 'code': 400 })
    if 'type' not in request.json:
        return jsonify({ 'error': "Required parameter 'type' is missing", 'code': 400 })
    if 'components' not in request.json:
        return jsonify({ 'error': "Required parameter 'components' is missing", 'code': 400 })

    name = request.json['name']
    description = request.json['description']
    _type = request.json['type']
    components = request.json['components']
    efficiency = 0

    if 'efficiency' in request.json:
        efficiency = request.json['efficiency']

    if isinstance(name, str) == False:
        return jsonify({ 'error': "Required parameter 'name' is not a valid string", 'code': 400 })
    if isinstance(description, str) == False:
        return jsonify({ 'error': "Required parameter 'description' is not a valid string", 'code': 400 })
    if isinstance(_type, int) == False:
        return jsonify({ 'error': "Required parameter 'type' is not a valid integer", 'code': 400 })
    if isinstance(components, list) == False:
        return jsonify({ 'error': "Required parameter 'components' is not a valid array", 'code': 400 })
    if isinstance(efficiency, int) == False:
        return jsonify({ 'error': "Optional parameter 'efficiency' is not a valid integer", 'code': 400 })

    if _type is not 0 and _type is not 1:
        return jsonify({ 'error': "Portfolio type must be 0 for Trading Portfolio or 1 for Industry Portfolio", 'code': 400 })

    if efficiency < 0 or efficiency > 100:
        return jsonify({ 'error': "Optional parameter 'efficiency' must be between 0 and 100", 'code': 400 })

    if _type == 1:
        if len(components) > 1:
            return jsonify({ 'error': "Industry portfolios must have a single manufacturable component", 'code': 400 })
    else:
        if len(components) > 20:
            return jsonify({ 'error': "Try a more reasonable number of components", 'code': 400 })

    used_ids = []
    _components = []
    industryQuantity = 0
    industryTypeID = 0
    manufacturedQuantity = 0

    try:
        if len(components) == 0:
            return jsonify({ 'error': "There are no components in your request", 'code': 400 })

        for component in components:
            if isinstance(component, dict) == False:
                return jsonify({ 'error': "Components must be vaild objects", 'code': 400 })

            if 'typeID' not in component:
                return jsonify({ 'error': "Component is missing required 'typeID' parameter", 'code': 400 })
            if 'quantity' not in component:
                return jsonify({ 'error': "Component is missing required 'quantity' parameter", 'code': 400 })

            if len(component.keys()) > 2:
                return jsonify({ 'error': "Component has invalid parameters", 'code': 400 })

            typeID = component['typeID']
            quantity = component['quantity']

            if isinstance(typeID, int) == False:
                return jsonify({ 'error': "Component 'typeID' is not a valid integer", 'code': 400 })

            if isinstance(quantity, int) == False:
                return jsonify({ 'error': "Component 'quantity' is not a valid integer", 'code': 400 })

            if typeID in used_ids:
                return jsonify({ 'error': "Component 'typeID' is duplicated. Each component must be unique", 'code': 400 })

            if typeID < 0 or typeID > 100000:
                return jsonify({ 'error': "Component 'typeID' is outside a reasonable range", 'code': 400 })

            if quantity < 0 or quantity > 1000000000:
                return jsonify({ 'error': "Component 'quantity' is outside a reasonable range", 'code': 400 })

            # Trading components will use the user supplied components and not duplicates
            if _type == 0:
                if str(typeID) not in market_ids:
                    return jsonify({ 'error': "Component 'typeID' is not a valid market item", 'code': 400 })

                used_ids.append(typeID)
                _components.append({'typeID': typeID, 'quantity': quantity})

            # Industry components are auto-selected based on the manufactured component the user requested
            else:
                if str(typeID) not in blueprints:
                    return jsonify({ 'error': "Component 'typeID' is not a valid manufacturable item", 'code': 400 })

                _blueprint = blueprints[str(typeID)]

                # Multiply the component requirements by the number of runs
                # Also consider the material efficiency
                for comp in _blueprint['materials']:
                    _components.append({'typeID': comp['typeID'], 'quantity':    math.ceil(comp['quantity'] * quantity * ((100.0 - efficiency) / 100.0))})

                industryQuantity = quantity
                industryTypeID = typeID

                # Multiply the manufactured quantity by the quantiy of the component the user is tracking
                # So if its 5 missile blueprints that each manufacture 100, the total quantiy is 500
                manufacturedQuantity = _blueprint['quantity'] * quantity

    except:
        traceback.print_exc()
        return jsonify({ 'error': "There is an error in the components array or the component is invalid", 'code': 400 })

    userPortfolioCount = portfolio_collection.find({'user_id': user_id}).count()

    if userPortfolioCount >= portfolio_limit:
        return jsonify({ 'error': "There is a limit of %s portfolios that a user can create. If you need this limit raised, contact an EVE Exchange admin." % portfolio_limit, 'code': 400 })

    try:
        portfolioCount = portfolio_collection.find().count()
        if portfolioCount > 0:
            portfolioMax = list(portfolio_collection.find().sort('portfolioID', DESCENDING))[0]
            portfolioID = 1 if portfolioMax is None else portfolioMax['portfolioID'] + 1
        else:
            portfolioID = 1

        portfolio_doc = {
            'name': name,
            'description': description,
            'type': _type,
            'efficiency': efficiency,
            'components': _components,
            'user_id': user_id,
            'portfolioID': portfolioID,
            'time': datetime.utcnow(),
            'hourlyChart': [],
            'dailyChart': [],
            'currentValue': 0,
            'averageSpread': 0,
            'industryQuantity': industryQuantity,
            'industryTypeID': industryTypeID,
            'industryValue': 0,
            'startingValue': 0,
            'manufacturedQuantity': manufacturedQuantity
        }

        portfolio_collection.insert(portfolio_doc)

        requests.post('http://localhost:4501/publish/portfolios/%s' % user_id, timeout=1)

    except:
        traceback.print_exc()
        return jsonify({ 'error': "There was an error with creating your portfolio", 'code': 400 })

    return jsonify({ 'message': 'Your new portfolio has been created with an id of %s' % portfolioID })

@app.route('/portfolio/delete/<int:id>', methods=['POST'])
@verify_jwt
def portfolio_delete(id, user_id, settings):

    try:
        portfolio = portfolio_collection.find_one({'user_id': user_id, 'portfolioID': id})

        if portfolio is None:
            raise Exception()

    except Exception:
        return jsonify({ 'error': "Failed to look up your portfolio. Double check that you have the correct portfolio ID", 'code': 400 })

    try:
        portfolio_collection.remove({'user_id': user_id, 'portfolioID': id}, multi=False)

        requests.post('http://localhost:4501/publish/portfolios/%s' % user_id, timeout=1)
    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while processing your deletion request", 'code': 400 })

    return jsonify({ 'message': 'Your portfolio has been deleted' })

@app.route('/subscription/subscribe', methods=['POST'])
@verify_jwt
def subscription_subscribe(user_id, settings):

    subscription = None
    cost = 150000000

    try:
        subscription = subscription_collection.find_one({'user_id': user_id})

        if subscription is None:
            raise Exception()
    except:
        return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

    is_premium = subscription['premium']

    if is_premium is not None:
        if is_premium == True:
            return jsonify({ 'error': "Your current subscription status is already premium", 'code': 400 })

    balance = subscription['balance']

    if cost > balance:
     return jsonify({ 'error': "Insufficient balance", 'code': 400 })

    try:
        subscription_collection.find_and_modify({'user_id': user_id}, {
            '$set': {
                'premium': True,
                'subscription_date': datetime.utcnow()
            },
            '$inc': {
                'balance': -cost
            },
            '$push': {
                'history': {
                    'time': datetime.utcnow(),
                    'type': 1,
                    'amount': cost,
                    'description': 'Subscription fee',
                    'processed': True
                }
            }
        })

        settings_collection.find_and_modify({'user_id': user_id}, {
            '$set': {
                'premium': True,
            },
        })

        requests.post('http://localhost:4501/publish/subscription/%s' % user_id, timeout=1)

    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while processing your subscription request. This should be reported", 'code': 400 })

    return jsonify({ 'message': 'Your subscription status has been updated' })

@app.route('/subscription/unsubscribe', methods=['POST'])
@verify_jwt
def subscription_unsubscribe(user_id, settings):

    subscription = None

    try:
        subscription = subscription_collection.find_one({'user_id': user_id})

        if subscription is None:
            raise Exception()
    except:
        return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

    is_premium = subscription['premium']

    if is_premium is not None:
        if is_premium == False:
            return jsonify({ 'error': "Your current subscription status is not premium", 'code': 400 })

    try:
        subscription_collection.find_and_modify({'user_id': user_id}, {
            '$set': {
                'premium': False,
                'subscription_date': None
            }
        })

        settings_collection.find_and_modify({'user_id': user_id}, {
            '$set': {
                'premium': False,
            },
        })

        requests.post('http://localhost:4501/publish/subscription/%s' % user_id, timeout=1)

    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while processing your subscription request. This should be reported", 'code': 400 })

    return jsonify({ 'message': 'Your subscription status has been updated' })

@app.route('/subscription/withdraw/<int:amount>', methods=['POST'])
@verify_jwt
def subscription_withdraw_amount(amount, user_id, settings):

    subscription = None

    try:
        subscription = subscription_collection.find_one({'user_id': user_id})

        if subscription is None:
            raise Exception()
    except:
        return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

    balance = subscription['balance']

    if amount < 0 or amount > balance:
     return jsonify({ 'error': "Insufficient balance", 'code': 400 })

    try:
        subscription_collection.find_and_modify({'user_id': user_id}, {
            '$inc': {
                'balance': -amount
            },
            '$push': {
                'history': {
                    'time': datetime.utcnow(),
                    'type': 1,
                    'amount': amount,
                    'description': 'Manual withdrawal request',
                    'processed': False
                }
            }
        })

        requests.post('http://localhost:4501/publish/subscription/%s' % user_id, timeout=1)

    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while processing your withdrawal request. This should be reported", 'code': 400 })

    return jsonify({ 'message': 'Your withdrawal request has been submitted' })

@app.route('/notification/<string:not_id>/read', methods=['POST'])
@verify_jwt
def notification_set_read(not_id, user_id, settings):

    notification = None

    try:
        notification = notification_collection.find_one({'_id': ObjectId(oid=not_id), 'user_id': user_id})

        if notification is None:
            raise Exception()
    except:
        return jsonify({ 'error': "Failed to look up the notification %s" % not_id, 'code': 400 })

    try:
        notification_collection.find_and_modify({'_id': ObjectId(oid=not_id), 'user_id': user_id}, {
            '$set': {
                'read': True
            }
        })

        requests.post('http://localhost:4501/publish/notifications/%s' % user_id, timeout=1)

    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while updating your notification. This should be reported", 'code': 400 })

    return jsonify({ 'message': 'Notification status is updated' })

@app.route('/notification/all/read', methods=['POST'])
@verify_jwt
def notification_all_read(user_id, settings):

    try:
        notification_collection.update({'user_id': user_id}, {
            '$set': {
                'read': True
            }
        }, multi=True)

        requests.post('http://localhost:4501/publish/notifications/%s' % user_id, timeout=1)

    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while updating your notification. This should be reported", 'code': 400 })

    return jsonify({ 'message': 'Notification statuses are updated' })

@app.route('/notification/<string:not_id>/unread', methods=['POST'])
@verify_jwt
def notification_set_unread(not_id, user_id, settings):

    notification = None

    try:
        notification = notification_collection.find_one({'_id': ObjectId(oid=not_id), 'user_id': user_id})

        if notification is None:
            raise Exception()
    except:
        return jsonify({ 'error': "Failed to look up the notification %s" % not_id, 'code': 400 })

    try:
        notification_collection.find_and_modify({'_id': ObjectId(oid=not_id), 'user_id': user_id}, {
            '$set': {
                'read': False
            }
        })

        requests.post('http://localhost:4501/publish/notifications/%s' % user_id, timeout=1)

    except Exception:
        traceback.print_exc()
        return jsonify({ 'error': "There was a database error while updating your notification. This should be reported", 'code': 400 })

    return jsonify({ 'message': 'Notification status is updated' })

# API Keys

@app.route('/apikey/add', methods=['POST'])
@verify_jwt
def apikey_add(user_id, settings):

    if request.is_json == False:
        return jsonify({ 'error': "Request must be in json format", 'code': 400 })

    try:
        if request.json is None:
            return jsonify({ 'error': "Request must be in json format", 'code': 400 })

    except:
        return jsonify({ 'error': "There was a problem parsing your json request", 'code': 400 })

    if 'type' not in request.json:
        return jsonify({ 'error': "Required parameter 'type' is missing", 'code': 400 })
    if 'keyID' not in request.json:
        return jsonify({ 'error': "Required parameter 'keyID' is missing", 'code': 400 })
    if 'vCode' not in request.json:
        return jsonify({ 'error': "Required parameter 'vCode' is missing", 'code': 400 })

    keyID = request.json['keyID']
    vCode = request.json['vCode']
    type = request.json['type']

    if isinstance(keyID, str) == False:
        return jsonify({ 'error': "Required parameter 'keyID' is not a valid string", 'code': 400 })
    if isinstance(vCode, str) == False:
        return jsonify({ 'error': "Required parameter 'vCode' is not a valid string", 'code': 400 })
    if isinstance(type, int) == False:
        return jsonify({ 'error': "Required parameter 'type' is not a valid integer", 'code': 400 })

    if type is not 0 and type is not 1:
        return jsonify({ 'error': "type must be 0 for character key or 1 for corporation key", 'code': 400 })

    if type == 0 and 'characterID' not in request.json:
        return jsonify({ 'error': "Required parameter 'characterID' is missing", 'code': 400 })
    elif type == 0:
        characterID = request.json['characterID']

        if isinstance(characterID, int) == False:
            return jsonify({'error': "Required parameter 'characterID' is not a valid integer", 'code': 400})
    else:
        characterID = 0

    if type == 1 and 'walletKey' not in request.json:
        return jsonify({'error': "Required parameter 'walletKey' is missing", 'code': 400})
    elif type == 1:
        walletKey = request.json['walletKey']

        if isinstance(walletKey, int) == False:
            return jsonify({'error': "Required parameter 'walletKey' is not a valid integer", 'code': 400})

        if walletKey < 1000 or walletKey > 1006:
            return jsonify({'error': "Corporation wallet division key must be 1000 - 1006", 'code': 400})
    else:
        walletKey = 0

    characterName = ""
    corporationID = 0
    corporationName = ""

    # Grab key info from EVE and verify it matches
    try:
        res = requests.get('https://api.eveonline.com/account/APIKeyInfo.xml.aspx?keyID=%s&vCode=%s' % (keyID, vCode), timeout=10)

        tree = ET.fromstring(res.text)

        if tree.find('error') is not None:
            raise Exception()

        keyResult = tree.find('result').find('key')
        rows = list(keyResult.find('rowset'))

        # TODO: Check expiry & correct corp mask
        if type == 0:
            if keyResult.attrib['type'] != "Character" and keyResult.attrib['type'] != "Account":
                return jsonify({'error': "Failed to verify that this key is the correct type of 'Character' or 'Account'", 'code': 400})
            #if keyResult.attrib['accessMask'] != "23072779":
            #    return jsonify({'error': "Failed to verify that the access mask is 23072779", 'code': 400})
            found = False
            for row in rows:
                if row.attrib['characterID'] == str(characterID):
                    found = True
                    characterName = row.attrib['characterName']
                    corporationName = row.attrib['corporationName']
                    corporationID = 0 if row.attrib['corporationID'] == "0" else int(row.attrib['corporationID'])
                    break
            if found is False:
                return jsonify({'error': "The requested characterID is not associated with this api key", 'code': 400})
        else:
            if keyResult.attrib['type'] != "Corporation":
                return jsonify({'error': "Failed to verify that this key is the correct type of 'Corporation'", 'code': 400})
            if keyResult.attrib['accessMask'] != "3149835":
                return jsonify({'error': "Failed to verify that the access mask is 3149835", 'code': 400})
            row = rows[0] # there is only 1 entry in this case
            characterID = int(row.attrib['characterID'])
            characterName = row.attrib['characterName']
            corporationName = row.attrib['corporationName']
            corporationID = int(row.attrib['corporationID'])

    except:
        traceback.print_exc()
        return jsonify({'error': "Failed to verify the given keyID and vCode", 'code': 400})

    # Verify that the api key the user is adding does not already exist
    if 'profiles' not in settings:
        return jsonify({'error': "Your account is not set up yet to accept API keys. Try re-logging into the website or report this to Maxim Stride", 'code': 400})

    for key in settings['profiles']:

        if keyID == key['key_id'] and vCode == key['vcode']:
            return jsonify({'error': "The given api key is already attached to your account", 'code': 400})

        if type == 0:
            if characterID == key['character_id'] and key['type'] == 0:
                return jsonify({'error': "The given character already has an API key on your account", 'code': 400})
        else:
            if corporationID == key['corporation_id'] and key['type'] == 1:
                return jsonify({'error': "The given corporation already has an API key on your account", 'code': 400})

    # Sanity checks
    if characterID is 0:
        return jsonify({'error': "There was an unknown problem grabbing the correct information for this api key", 'code': 400})

    # Add api key to account now
    settings_collection.find_and_modify({'user_id': user_id}, {
            '$push': {
                'profiles': {
                    'type': type,
                    'key_id': keyID,
                    'vcode': vCode,
                    'character_id': characterID,
                    'character_name': characterName,
                    'corporation_id': corporationID,
                    'corporation_name': corporationName,
                    'wallet_balance': 0,
                    'wallet_key': walletKey,
                    'id': str(ObjectId()) # unique ID to be used by api's
                }
            }
    })

    try:
        requests.post('http://localhost:4501/publish/settings/%s' % user_id, timeout=1)
    except:
        traceback.print_exc()

    return jsonify({'message': 'API key has been added to your account'})

@app.route('/apikey/remove/<string:key_id>', methods=['GET', 'POST'])
@verify_jwt
def apikey_remove(key_id, user_id, settings):

    if isinstance(key_id, str) == False:
        return jsonify({ 'error': "Required parameter 'id' is not a valid string", 'code': 400 })

    # Verify that the api key the user is adding does not already exist
    if 'profiles' not in settings:
        return jsonify({'error': "Your account is not set up yet to accept API keys. Try re-logging into the website or report this to Maxim Stride", 'code': 400})

    found = False

    # Verify the requested key exists
    try:
        for key in settings['profiles']:
            if 'id' in key and key_id == key['id']:
                found = True

    except:
        return jsonify({'message': 'There was a problem removing the API key. Please report this to Maxim Stride'})

    if not found:
        return jsonify({'message': 'Failed to find the requested API key. Make sure to use its unique ID in the request'})

    # Query to remove the exact array element
    settings_collection.find_and_modify({'user_id': user_id}, {
            '$pull': {
                'profiles': {
                    'id': key_id
                }
            }
    })

    try:
        requests.post('http://localhost:4501/publish/settings/%s' % user_id, timeout=1)
    except:
        traceback.print_exc()

    return jsonify({'message': 'API key has been removed from your account'})

@app.route('/apikey/get', methods=['GET', 'POST'])
@verify_jwt
def apikey_get_all(user_id, settings):

    # Verify that the api key the user is adding does not already exist
    if 'profiles' not in settings:
        return jsonify({'error': "Your account is not set up yet to accept API keys. Try re-logging into the website or report this to Maxim Stride", 'code': 400})

    return jsonify(settings['profiles'])

@app.route('/apikey/get/<string:key_id>', methods=['GET', 'POST'])
@verify_jwt
def apikey_get_one(key_id, user_id, settings):

    if isinstance(key_id, str) == False:
        return jsonify({ 'error': "Required parameter 'id' is not a valid string", 'code': 400 })

    # Verify that the api key the user is adding does not already exist
    if 'profiles' not in settings:
        return jsonify({'error': "Your account is not set up yet to accept API keys. Try re-logging into the website or report this to Maxim Stride", 'code': 400})

    try:
        for key in settings['profiles']:
            if 'id' in key and key_id == key['id']:
                return jsonify(key)

    except:
        return jsonify({'message': 'There was a problem retrieving the API key. Please report this to Maxim Stride'})

    return jsonify({'message': 'Failed to find the requested API key. Make sure to use its unique ID in the request'})

# SDE

@app.route('/sde/blueprints', methods=['GET'])
def sde_blueprints():

    return Response(blueprints_json)

@app.route('/sde/marketgroups', methods=['GET'])
def sde_marketgroups():

    return Response(market_groups_json)

# OAuth

@app.route('/oauth')
def do_oauth():
    return evesso.authorize(callback=url_for('do_oauth_authorized', _external=True))

@app.route('/oauth/verify')
def do_oauth_authorized():
    resp = evesso.authorized_response()
    if resp is None or resp.get('access_token') is None:
        return 'Access denied: reason=%s error=%s resp=%s' % (
            request.args['error'],
            request.args['error_description'],
            resp
        )

    session['evesso_token'] = (resp['access_token'], '')

    me = evesso.get('oauth/verify')

    token = jwt.encode({'user_id': me.data['CharacterID'],
                        'user_name': me.data['CharacterName'],
                        'exp': datetime.utcnow() + timedelta(hours=24)
    }, auth_jwt_secret, algorithm='HS256')

    return redirect('%s/?token=%s' % (redirect_host, token.decode('ascii')), code=302)

@evesso.tokengetter
def get_evesso_oauth_token():
    return session.get('evesso_token')

# Deepstream

def insert_defaults(user_id, user_name):
    user_doc = {
        'user_id': user_id,
        'user_name': user_name,
        'admin': False,
        'last_online': datetime.now(),
        'join_date': datetime.now()
    }

    settings_doc = {
        'user_id': user_id,
        'premium': False,
        'api_key': str(ObjectId()),
        'profiles': []
    }

    profit_alltime = {
        "alltime": {
            "broker": 0,
            "profit": 0,
            "taxes": 0
        },
        "biannual": {
            "broker": 0,
            "profit": 0,
            "taxes": 0
        },
        "day": {
            "broker": 0,
            "profit": 0,
            "taxes": 0
        },
        "month": {
            "broker": 0,
            "profit": 0,
            "taxes": 0
        },
        "user_id": user_id,
        "week": {
            "broker": 0,
            "profit": 0,
            "taxes": 0
        }
    }

    profit_items = {
        "user_id": user_id,
        "items": [],
        'profiles': []
    }

    subscription_doc = {
        "user_id": user_id,
        "premium": False,
        "balance": 0,
        "history": [],
        "subscription_date": None,
        "user_name": user_name
    }

    beta_notification = {
        "user_id": user_id,
        "time": datetime.utcnow(),
        "read": False,
        "message": "Welcome to EVE Exchange! Please report any problems you find to Maxim Stride or @maxim on Tweetfleet. Happy trading."
    }

    mongo_db.users.insert(user_doc)
    mongo_db.settings.insert(settings_doc)
    mongo_db.profit_alltime.insert(profit_alltime)
    mongo_db.profit_top_items.insert(profit_items)
    mongo_db.subscription.insert(subscription_doc)
    mongo_db.notifications.insert(beta_notification)

    # Publish the new account creation
    requests.post('http://localhost:4501/publish/subscription/%s' % user_id, timeout=1)

    return user_doc, settings_doc

@app.route('/deepstream/authorize', methods=['POST'])
def do_deepstream_authorize():

    try:
        if request.is_json == False:
            return 'Invalid credentials', 403
    except:
        return 'Invalid request format', 403

    _data = None
    user_doc = None
    settings_doc = None

    try:
        if 'authData' not in request.json:
            return 'Invalid credentials', 403

        if 'admin' in request.json['authData']:
            if request.json['authData']['admin'] == admin_secret:
                return jsonify({'username': 'admin', 'clientData': {'user_name':'admin'}, 'serverData': {'admin':True}})

        if 'token' not in request.json['authData']:
            return 'Invalid credentials', 403

        if isinstance(request.json['authData']['token'], str) == False:
            return 'Invalid credentials', 403

        _data = jwt.decode(request.json['authData']['token'], auth_jwt_secret)

        user_doc = mongo_db.users.find_one({'user_id': _data['user_id']})

        if user_doc is None:

            # Create new user data
            try:
                user_doc, settings_doc = insert_defaults(_data['user_id'], _data['user_name'])

                if '_id' in settings_doc:
                    del settings_doc['_id']
            except:
                traceback.print_exc()
                return 'Invalid credentials', 403

            # Publish new user
            requests.post('http://localhost:4501/user/create', json=settings_doc, timeout=1)
        else:
            settings_doc = mongo_db.settings.find_one({'user_id': _data['user_id']})
            mongo_db.users.update({'_id': user_doc['_id']}, { '$set': { 'last_online': datetime.now()}})

        requests.post('http://localhost:4501/user/login', json={'user_id': _data['user_id']}, timeout=1)

        # ID object can't be serialized to json
        if '_id' in user_doc:
            del user_doc['_id']
        if '_id' in settings_doc:
            del settings_doc['_id']

    except:
        traceback.print_exc()
        return 'Invalid credentials', 403

    if _data is None or user_doc is None or settings_doc is None:
        return 'Invalid credentials', 403

    client_data = {
        **{k:user_doc[k] for k in ['user_name', 'user_id', 'admin']},
        **{k:settings_doc[k] for k in ['premium']},
    }

    return jsonify({ 'username': _data['user_name'], 'clientData': client_data, 'serverData': {**user_doc, **settings_doc}})

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({ 'error': "Route not found", 'code': 404 })

@app.errorhandler(403)
def not_found(error):
    return jsonify({ 'error': "Failed to validate your authentication token or api key", 'code': 403 })

@app.errorhandler(401)
def not_found(error):
    return jsonify({ 'error': "Failed to validate your authentication token or api key", 'code': 401 })

@app.errorhandler(405)
def not_found(error):
    return jsonify({ 'error': "Method or endpoint is not allowed", 'code': 405 })

# Start server
if __name__ == '__main__':
    if debug:
        app.run(debug=debug, port=port, host='0.0.0.0', threaded=False)
    else:
        print("Running in production WSGI mode on port %s" % port)
        http_server = WSGIServer(('', port), app)
        http_server.serve_forever()