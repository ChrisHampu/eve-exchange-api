import os
import sys
import requests
import rethinkdb as r
import redis
import traceback
import math
import time
import json
from flask import Flask, Response, request, abort, jsonify, current_app
from flask_cors import CORS
from functools import wraps

# Configuration
etf_db = 'evetradeforecaster'
etf_host = 'localhost'

settings_table = 'user_settings'
subscription_table = 'subscription'
portfolios_table = 'portfolios'
users_table = 'users'

portfolio_limit = 10 # Max number of portfolios a user can have

port = 5000
verify_port = 4501
env = os.environ.get('ETF_API_ENV', 'development')

debug = False if env == 'production' else True

# Application
app = Flask(__name__)
CORS(app)

re = None

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

try:
  re = redis.StrictRedis(host=etf_host, port=6379, db=0)
except:
  print("Redis server is unavailable")

# Utility to get connections to rethinkDB
def getConnection():
  return r.connect(db=etf_db, host=etf_host)

# Decorator to validate a JWT and retrieve the users info from rethinkDB
def verify_jwt(fn):
  @wraps(fn)
  def wrapper(*args, **kwargs):

    res = None

    if request.is_json == False:
      return jsonify({ 'error': "Request must be in json format", 'code': 400 })

    if res is None:
      try:
        auth_header = request.headers.get('Authorization')

        if auth_header == None:
          auth_header = request.headers.get('authorization')

        if auth_header == None:
          return jsonify({ 'error': "Authorization header is missing", 'code': 400 })

        split = auth_header.split(" ")

        if len(split) != 2:
          return jsonify({ 'error': "Failed to parse authorization header", 'code': 400 })

        if split[0] != "Token":
          return jsonify({ 'error': "Failed to parse authorization header", 'code': 400 })

        res = requests.post('http://evetradeforecaster.com:%s/verify' % verify_port, json={'jwt': split[1]})

      except:
        traceback.print_exc()
        return jsonify({ 'error': "Failed to parse authorization header", 'code': 400 })

    if res is None:
      return jsonify({ 'error': "Failed to parse authorization header", 'code': 400 })

    user = None
    doc = res.json()

    # failed to validate the jwt
    if 'error' in doc:
      abort(401)

    jwt = doc['jwt']
    doc_id = jwt['id']

    try:
      user = r.table(users_table).get(doc_id).run(getConnection())
      if user is None:
        raise Exception()
    except:
      return jsonify({ 'error': "Failed to look up your user information", 'code': 400 })

    user_id = user['user_id']

    try:
      user_settings = list(r.table(settings_table).filter(lambda doc: doc['userID'] == user_id).limit(1).run(getConnection()))
      if len(user_settings) == 0:
        raise Exception()
    except:
      traceback.print_exc()
      return jsonify({ 'error': "Failed to look up your user settings", 'code': 400 })

    return fn(user_id=user_id, settings=user_settings[0], *args, **kwargs)

  return wrapper

# Routes
@app.route('/', methods=['GET'])
def index():
  return current_app.send_static_file('api.html')

@app.route('/schemas', methods=['GET'])
def schemas():
  return jsonify({
    'message': "EVE Trade Forecaster API"
  })

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

        _components = _blueprint['materials']

        # Multiply the component requirements by the number of runs
        # Also consider the material efficiency
        for comp in _components:
          comp['quantity'] = math.ceil(comp['quantity'] * quantity * ((100 - efficiency) / 100))

        industryQuantity = quantity
        industryTypeID = typeID

        # Multiply the manufactured quantity by the quantiy of the component the user is tracking
        # So if its 5 missile blueprints that each manufacture 100, the total quantiy is 500
        manufacturedQuantity = _blueprint['quantity'] * quantity

  except:
    traceback.print_exc()
    return jsonify({ 'error': "There is an error in the components array or the component is invalid", 'code': 400 })

  userPortfolioCount = r.table(portfolios_table).filter({'userID': user_id}).count().run(getConnection())

  if userPortfolioCount >= portfolio_limit:
    return jsonify({ 'error': "There is a limit of %s portfolios that a user can create. If you need this limit raised, contact an EVE Trade Forecaster admin." % portfolio_limit, 'code': 400 })

  try:

    portfolioCount = r.table(portfolios_table).count().run(getConnection())

    if portfolioCount > 0:
      portfolioMax = r.table(portfolios_table).max('portfolioID').run(getConnection())
      portfolioID = 1 if portfolioMax is None else portfolioMax['portfolioID'] + 1
    else:
      portfolioID = 1
    
    r.table(portfolios_table).insert({
      'name': name,
      'description': description,
      'type': _type,
      'efficiency': efficiency,
      'components': _components,
      'userID': user_id,
      'portfolioID': portfolioID,
      'time': r.now(),
      'hourlyChart': [],
      'dailyChart': [],
      'currentValue': 0,
      'averageSpread': 0,
      'industryQuantity': industryQuantity,
      'industryTypeID': industryTypeID,
      'industryValue': 0,
      'startingValue': 0,
      'manufacturedQuantity': manufacturedQuantity
    }).run(getConnection())
    
  except:
    traceback.print_exc()
    return jsonify({ 'error': "There was an error with creating your portfolio", 'code': 400 })

  return jsonify({ 'message': 'Your new portfolio has been created with an id of %s' % portfolioID })

@app.route('/portfolio/delete/<int:id>', methods=['POST'])
@verify_jwt
def portfolio_delete(id, user_id, settings):

  portfolio = None

  try:
    portfolio = list(r.table(portfolios_table).filter(lambda doc: (doc['userID'] == user_id) & (doc['portfolioID'] == id)).limit(1).run(getConnection()))
    if portfolio is None or len(portfolio) == 0:
      raise Exception()
    portfolio = portfolio[0]
  except Exception:
    return jsonify({ 'error': "Failed to look up your portfolio. Double check that you have the correct portfolio ID", 'code': 400 })

  try:
    r.table(portfolios_table).filter(lambda doc: (doc['userID'] == user_id) & (doc['portfolioID'] == id)).limit(1).delete().run(getConnection())
  except Exception:
    traceback.print_exc()
    return jsonify({ 'error': "There was a database error while processing your deletion request", 'code': 400 })

  return jsonify({ 'message': 'Your portfolio has been deleted' })

@app.route('/subscription', methods=['POST'])
def subscription():
  return jsonify({
    'endpoints': {
      'withdraw': {
        'description': 'Actions relating to your EVE Trade Forecaster account',
        'method': 'POST',
        'response': {
          '$ref': 'endpoints'
        }
      }
    } 
  })

@app.route('/subscription/subscribe', methods=['POST'])
@verify_jwt
def subscription_subscribe(user_id, settings):

  subscription = None
  cost = 150000000

  try:
    subscription = list(r.table(subscription_table).filter(lambda doc: doc['userID'] == user_id).limit(1).run(getConnection()))
    if subscription is None or len(subscription) == 0:
      raise Exception()
    subscription = subscription[0]
  except Exception:
    return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

  is_premium = subscription['premium']

  if is_premium is not None:
    if is_premium == True:
      return jsonify({ 'error': "Your current subscription status is already premium", 'code': 400 })

  balance = subscription['balance']

  if cost > balance:
   return jsonify({ 'error': "Insufficient balance", 'code': 400 })

  try:

    r.table(subscription_table).get(subscription['id']).update({
      'balance': r.row['balance'] - cost,
      'history': r.row['history'].append({
        'time': r.now(),
        'type': 1,
        'amount': cost,
        'description': 'Subscription fee',
        'processed': True
      }),
      'premium': True,
      'subscription_date': r.now()
    }).run(getConnection())

    r.table(users_table).filter({'user_id': user_id}).limit(1).update({'groups': r.branch(r.row["groups"].contains("premium"), r.row["groups"], r.row['groups'].append('premium'))}).run(getConnection())

  except Exception:
    traceback.print_exc()
    return jsonify({ 'error': "There was a database error while processing your subscription request. This should be reported", 'code': 400 })

  return jsonify({ 'message': 'Your subscription status has been updated' })

@app.route('/subscription/unsubscribe', methods=['POST'])
@verify_jwt
def subscription_unsubscribe(user_id, settings):

  subscription = None

  try:
    subscription = list(r.table(subscription_table).filter(lambda doc: doc['userID'] == user_id).limit(1).run(getConnection()))
    if subscription is None or len(subscription) == 0:
      raise Exception()
    subscription = subscription[0]
  except Exception:
    return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

  is_premium = subscription['premium']

  if is_premium is not None:
    if is_premium == False:
      return jsonify({ 'error': "Your current subscription status is not premium", 'code': 400 })

  try:
    r.table(subscription_table).get(subscription['id']).update({
      'premium': False,
      'subscription_date': None
    }).run(getConnection())

    active = list(r.table(users_table).filter({'user_id': user_id}).limit(1).run(getConnection()))

    if len(active) != 1:
      return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

    active = active[0]

    if 'premium' in active['groups']:
      active['groups'].remove('premium')

    r.table(users_table).filter({'user_id': user_id}).limit(1).update({'groups': active['groups']}).run(getConnection())

  except Exception:
    traceback.print_exc()
    return jsonify({ 'error': "There was a database error while processing your subscription request. This should be reported", 'code': 400 })

  return jsonify({ 'message': 'Your subscription status has been updated' })

@app.route('/subscription/withdraw', methods=['POST'])
def subscription_withdraw():
  return jsonify({
    'endpoints': {
      '<int:amount>': {
        'description': 'Request a withdrawal of "amount" from your EVE Trade Forecaster balance',
        'method': 'POST',
        'response': 'message'
      }
    } 
  })

@app.route('/subscription/withdraw/<int:amount>', methods=['POST'])
@verify_jwt
def subscription_withdraw_amount(amount, user_id, settings):

  subscription = None

  try:
    subscription = list(r.table(subscription_table).filter(lambda doc: doc['userID'] == user_id).limit(1).run(getConnection()))
    if subscription is None or len(subscription) == 0:
      raise Exception()
    subscription = subscription[0]
  except Exception:
    return jsonify({ 'error': "Failed to look up your subscription status", 'code': 400 })

  balance = subscription['balance']

  if amount < 0 or amount > balance:
   return jsonify({ 'error': "Insufficient balance", 'code': 400 })

  try:
    r.table(subscription_table).get(subscription['id']).update({
      'balance': r.row['balance'] - amount,
      'history': r.row['history'].append({
        'time': r.now(),
        'type': 1,
        'amount': amount,
        'description': 'Manual withdrawal request',
        'processed': False
      })
    }).run(getConnection())
  except Exception:
    traceback.print_exc()
    return jsonify({ 'error': "There was a database error while processing your withdrawal request. This should be reported", 'code': 400 })

  return jsonify({ 'message': 'Your withdrawal request has been submitted' })

@app.route('/sde/blueprints', methods=['GET'])
def sde_blueprints():

  return Response(blueprints_json)

@app.route('/sde/marketgroups', methods=['GET'])
def sde_marketgroups():

  return Response(market_groups_json)

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
  app.run(debug=debug, port=port, host='0.0.0.0')