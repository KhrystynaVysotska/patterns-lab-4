import os
from flask import Flask, request, render_template, Response

from etl.etl import ETL
from helpers import format_sse, get_filename_from_url
from etl.loader import EventHubLoadStrategy, ConsoleLoadStrategy, FileLoadStrategy
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
        strategy = os.environ["LOAD_STRATEGY"]

        if strategy == 'FILE_STRATEGY':
            filename = get_filename_from_url(datasource_url)
            load_strategy = FileLoadStrategy(filename)
        elif strategy == 'EVENT_HUB_STRATEGY':
            load_strategy = EventHubLoadStrategy()
        else:
            load_strategy = ConsoleLoadStrategy()

        etl = ETL(load_strategy, datasource_url)

        for progress_msg in etl.start():
            msg = format_sse(data=progress_msg["msg"], event=progress_msg["level"])
            announcer.announce(msg=msg)

        return {}, 204
    else:
        return render_template('upload.html')


if __name__ == '__main__':
    app.run(debug=True)
