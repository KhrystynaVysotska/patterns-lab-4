import os

from flask import Flask, request, render_template, Response

from etl import ETL
from helpers import format_sse
from loader import EventHubLoadStrategy, ConsoleLoadStrategy
from pubsub import MessageAnnouncer

app = Flask(__name__)
app.secret_key = os.environ['APP_KEY']

announcer = MessageAnnouncer()


@app.route('/listen', methods=['GET'])
def listen():
    def stream():
        messages = announcer.listen()
        while True:
            msg = messages.get()
            yield msg

    return Response(stream(), mimetype='text/event-stream')


@app.route('/', methods=['GET', 'POST'])
def process_data():
    if request.method == 'POST':
        datasource_url = request.form['url']

        if os.environ["LOAD_STRATEGY"] == 'EVENT_HUB':
            configs = {"conn_str": os.environ["EVENT_HUB_CONNECTION_STRING"],
                       "eventhub_name": os.environ['EVENT_HUB_NAME']}
            load_strategy = EventHubLoadStrategy(configs)
        else:
            load_strategy = ConsoleLoadStrategy()

        etl = ETL(load_strategy, datasource_url)

        for progress_msg in etl.start():
            msg = format_sse(data=progress_msg["msg"], event=progress_msg["category"])
            announcer.announce(msg=msg)

        return {}, 204
    else:
        return render_template('upload.html')


if __name__ == '__main__':
    app.run(debug=True)
