import os
import sys
import requests
import rethinkdb as r
import redis
from flask import Flask, request, abort, jsonify
from flask_cors import CORS
from functools import wraps

# Configuration
etf_db = os.environ.get('ETF_DB', 'evetradeforecaster')
etf_host = os.environ.get('ETF_DB_HOST', '149.202.187.40')
etf_internal_db = os.environ.get('ETF_INTERNAL_DB', 'evetradeforecaster_internal')

settings_table = os.environ.get('ETF_SETTINGS_TABLE', 'user_settings_664c29459b15')
subscription_table = os.environ.get('ETF_SUBSCRIPTION_TABLE', 'subscription_ce235ce22d6e')
users_table = 'users'

port = int(os.environ.get('ETF_API_PORT', 5000))
verify_port = int(os.environ.get('VERIFY_PORT', 3001))
env = os.environ.get('ETF_API_ENV', 'development')

debug = False if env == 'production' else True

# Application
app = Flask(__name__)
CORS(app)

re = None

try:
  re = redis.StrictRedis(host='localhost', port=6379, db=0)
except:
  print("Redis server is unavailable")

# Utilities to get connections to rethinkDB
def getConnection():
  return r.connect(db=etf_db, host=etf_host)

def getInternalConnection():
  return r.connect(db=etf_internal_db, host=etf_host)

# Decorator to validate a JWT and retrieve the users info from rethinkDB
def verify_jwt(fn):
  @wraps(fn)
  def wrapper(*args, **kwargs):

    if request.is_json == False:
      return jsonify({ 'error': "Request must be in json format", 'code': 400 })

    try:
      if request.json is not None:
        if 'jwt' not in request.json:
          return jsonify({ 'error': "A 'jwt' field must be passed with a valid JSON Web Token for authentication", 'code': 400 })

        res = requests.post('http://localhost:%s/verify' % verify_port, json={'jwt': request.json['jwt']})

    except:
      pass

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

      res = requests.post('http://localhost:%s/verify' % verify_port, json={'jwt': split[1]})

    except:
      return jsonify({ 'error': "Failed to parse authorization header", 'code': 400 })

    user = None
    doc = res.json()

    # failed to validate the jwt
    if 'error' in doc:
      abort(401)

    jwt = doc['jwt']
    doc_id = jwt['id']

    try:
      user = r.table(users_table).get(doc_id).run(getInternalConnection())
      if user is None:
        raise Exception()
    except:
      return jsonify({ 'error': "Failed to look up your user information", 'code': 400 })

    user_id = user['user_id']

    try:
      user_settings = list(r.table(settings_table).get_all([user_id], index='userID').limit(1).run(getConnection()))
      if len(user_settings) == 0:
        raise Exception()
    except:
      return jsonify({ 'error': "Failed to look up your user settings", 'code': 400 })

    return fn(user_id=user_id, settings=user_settings[0], *args, **kwargs)

  return wrapper

# Routes
@app.route('/', methods=['GET'])
def index():
  return jsonify({
    'message': "EVE Trade Forecaster API v1",
    'discovery': 'Endpoints are listed as relative to the current path. Parameterized endpoints require using the named parameter and type as the child path of the relevant endpoint.',
    'endpoints': {
      'subscription': {
        'description': 'Actions relating to your EVE Trade Forecaster account',
        'method': 'POST/GET',
        'response': {
          '$ref': 'endpoints'
        }
      }
    }
  })

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
    pip.hmget(k, ['type', 'spread', 'tradeVolume', 'buyFifthPercentile'])

  docs = pip.execute()

  # Find ideal matches to query params
  ideal = [doc[0] for doc in docs if float(doc[1]) >= minspread and float(doc[1]) <= maxspread and float(doc[2]) >= minvolume and float(doc[2]) <= maxvolume and float(doc[3]) >= minprice and float(doc[3]) <= maxprice ]

  # Pull out complete documents for all ideal matches

  pip = re.pipeline()

  for k in ideal:
    pip.hgetall('dly:'+k.decode('ascii'))

  # Execute and grab only wanted attributes
  docs = [{key:float(row[key]) for key in (b'type', b'spread', b'tradeVolume', b'buyFifthPercentile', b'spreadSMA', b'tradeVolumeSMA', b'sellFifthPercentile')} for row in pip.execute()]

  return jsonify(docs)

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
    subscription = list(r.table(subscription_table).get_all([user_id], index='userID').limit(1).run(getConnection()))
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
      'withdrawal_history': r.row['withdrawal_history'].append({
        'time': r.now(),
        'type': 1,
        'amount': amount,
        'description': 'Manual withdrawal request',
        'processed': False
      })
    }).run(getConnection())
  except Exception:
    return jsonify({ 'error': "There was a database error while processing your withdrawal request. This should be reported", 'code': 400 })

  return jsonify({ 'message': 'Your withdrawal request has been submitted' })

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
  app.run(debug=debug, port=port)