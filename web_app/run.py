from flask import Flask, render_template
from flask_bootstrap import Bootstrap

import json
import pickle
import redis
import config

redis_db = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=1)

def create_app():
  app = Flask(__name__)
  Bootstrap(app)

  return app

app = create_app()

@app.route("/")
def index():
    # groups is a list of lists, object = json question object
    keys = redis_db.keys()
    kv = {k:pickle.loads(r.get(k)) for k in keys}
	kv_items = kv.items()
	return render_template("base.html", questions=kv_items)
