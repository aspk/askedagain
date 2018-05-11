from app import app
from flask import render_template
import datetime
import redis
import math

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + "/src/config")
import config

rdb = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=0)


''' TEMP REDIS FUNCTIONS '''


def calc_likelihood(sim_score):
    likelihoods = [("Low", "btn-default"), ("Medium", "btn-warning"), ("High", "btn-danger")]
    partition = 0.8 / len(likelihoods)
    return likelihoods[math.floor(sim_score / partition)]

def so_link(qid):
    return "http://stackoverflow.com/q/{0}".format(qid)


def get_qinfo(tag, qid):
        q = eval(rdb.hget("lsh:{0}".format(tag), qid))
        return q["title"]


''' ROUTES '''


@app.route("/")
@app.route("/candidates")
def candidates():
    # dups = {}

    # candidate_sims = [(eval(dup[0]), dup[1]) for dup in rdb.zrangebyscore(
    #     "dup_cand:test_tag",
    #     str(config.DUP_QUESTION_MIN_HASH_THRESHOLD),
    #     "+inf",
    #     withscores=True)]
    # print(candidate_sims)

    # tag = "test_tag"
    # candidates = [
    #     (
    #         x[0][0],  # q1 qid
    #         get_qinfo(tag, x[0][0]),  # q1 title
    #         x[0][1],  # q2 qid
    #         get_qinfo(tag, x[0][1]),  # q2 title
    #         x[1]  # sim score
    #     )
    #     for tag in [tag] for x in candidate_sims
    # ]
    # # dups[tag.capitalize()] = candidates
    # candidates = [list(c) + [calc_likelhood(float(c[4]))] for c in candidates]
    # print(candidates)
    # dups["Javascript"] = candidates
    # return render_template("duplicates.html", dup_cands=dups)

    # Not Likely - btn-default
    # Could be btn-warning
    # Likely btn-danger
    # groups is a list of lists, object = json question object
    llh_rating, llh_button = calc_likelihood(0.2)
    dup_cands = [{
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p"),
        "tag": "Javascript",
        "q1_id": 24014531,
        "q2_id": 12098,
        "q1_link": so_link(24014531),
        "q2_link": so_link(12098),
        "q1": "What's the point of this?",
        "q2": "What's the point of that?",
        "likelihood_button": llh_button,
        "likelihood_rating": llh_rating
    }]
    return render_template("q_list.html", dup_cands=dup_cands)


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/visualization")
def visualization():
    return render_template("q_cluster_visualization.html")


@app.route("/metrics")
def metrics():
    return render_template("metrics.html")
