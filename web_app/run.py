from flask import Flask, render_template
from flask_bootstrap import Bootstrap

import json
import pickle
import redis
import config

redis_db = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=0)

def create_app():
  app = Flask(__name__)
  Bootstrap(app)

  return app

app = create_app()

@app.route("/")
def index():
	questions = [] if redis_db.get('mvp') is None else pickle.loads(redis_db.get('mvp'))
	print(questions)
	return render_template("base.html",questions=questions)

