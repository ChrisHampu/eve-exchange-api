import os
import sys
import requests
import rethinkdb as r
from flask import Flask, request, abort, jsonify

# Configuration
etf_db = os.environ.get('ETF_DB', 'evetradeforecaster')
etf_host = os.environ.get('ETF_DB_HOST', 'localhost')
etf_internal_db = os.environ.get('ETF_INTERNAL_DB', 'evetradeforecaster_internal')

settings_table = os.environ.get('ETF_SETTINGS_TABLE', 'user_settings_664c29459b15')
subscription_table = os.environ.get('ETF_SUBSCRIPTION_TABLE', 'subscription_ce235ce22d6e')
users_table = 'users'

port = int(os.environ.get('ETF_API_PORT', 5000))
env = os.environ.get('ETF_API_ENV', 'development')

debug = False if env == 'production' else True

# Application
app = Flask(__name__)

# Utilities to get connections to rethinkDB
def getConnection():
  return r.connect(db=etf_db, host=etf_host)

def getInternalConnection():
  return r.connect(db=etf_internal_db, host=etf_host)

# Decorator to validate a JWT and retrieve the users info from rethinkDB
def verify_jwt(fn):
  def wrapper(*args, **kwargs):
    if request.is_json == False or request.json == None:
      return jsonify({ 'error': "Not a valid JSON request", 'code': 400 })

    if 'jwt' not in request.json:
      return jsonify({ 'error': "A 'jwt' field must be passed with a valid JSON Web Token for authentication", 'code': 400 })

    res = requests.post('http://localhost:3001/verify', json={'jwt': request.json['jwt']})

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
    except Exception:
      return jsonify({ 'error': "Failed to look up your user information", 'code': 400 })

    user_id = user['user_id']

    try:
      user_settings = list(r.table(settings_table).get_all([user_id], index='userID').limit(1).run(getConnection()))
      if len(user_settings) == 0:
        raise Exception()
    except Exception:
      return jsonify({ 'error': "Failed to look up your user settings", 'code': 400 })

    return fn(user_id=user_id, settings=user_settings[0], *args, **kwargs)

  return wrapper

# Routes
@app.route('/', methods=['POST', 'GET'])
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

@app.route('/schemas', methods=['POST', 'GET'])
def schemas():
  return jsonify({
    'message': "EVE Trade Forecaster API v1"
  })

@app.route('/subscription', methods=['POST', 'GET'])
def subscription():
  return jsonify({
    'endpoints': {
      'withdraw': {
        'description': 'Actions relating to your EVE Trade Forecaster account',
        'method': 'POST/GET',
        'response': {
          '$ref': 'endpoints'
        }
      }
    } 
  })

@app.route('/subscription/withdraw', methods=['POST', 'GET'])
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
  return jsonify({ 'error': "Failed to validate your authentication token or api key", 'code': 403 })

@app.errorhandler(405)
def not_found(error):
  return jsonify({ 'error': "Method or endpoint is not allowed", 'code': 405 })

# Start server
if __name__ == '__main__':
  app.run(debug=debug, port=port)