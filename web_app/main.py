#!/usr/bin/env python
# from flask.ext.socketio import SocketIO
from app import app

socketio = SocketIO(app)
app.run(host='0.0.0.0', debug=True)

# if __name__ == "__main__":
#     socketio.run(app)
