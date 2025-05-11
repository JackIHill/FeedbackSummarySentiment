
import os
import time
from flask import Flask, render_template, url_for, request, make_response, session, redirect
import json
from api import api, get_all_operators, get_all_phrases
# 
# secret key should be random value on deployment
# 

process_operator_list = []
process_phrase_list = []
# below should be disctinct (x.proper() for x if not in)

existing_operators = get_all_operators()


# existing_phrases = ['Price or Value', 'Atmosphere', 'Food', 'Drink', 'Staff or Service', 'Portion Size']
existing_phrases = get_all_phrases()
# querying back to DB should use IDs for speed, so prob store them here with their text name in sqlalch
# db here and refresh every 30s or something?



selected_operators = []
selected_phrases = []
def create_app():
    app = Flask(__name__, static_folder = 'static')
    app.secret_key = 'dev'
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

    app.register_blueprint(api)
    


    @app.route('/', methods=['POST', 'GET'])
    def index():

        # session.clear()
        # session['selected_operators'] = [op for op in existing_operators]
        if request.method == 'POST':            
            operator_selected = [op for op in existing_operators if op['OperatorName'] in request.form.getlist('selection-operator')]
            phrase_selected = [ph for ph in existing_phrases if ph['Phrase'] in request.form.getlist('selection-phrase')]

            # process_phrase_list.extend(phrase_selected)

            if operator_selected:
                session['selected_operators'] = operator_selected

            if phrase_selected:
                session['selected_phrases'] = phrase_selected

            return '', 204
        
        else:

            selected_operators = session.setdefault('selected_operators', existing_operators[:])
            selected_phrases = session.setdefault('selected_phrases', existing_phrases[:])

            return render_template('index.html',
                                    existing_operators=existing_operators,
                                    selected_operators=selected_operators,
                                    existing_phrases=existing_phrases,
                                    selected_phrases=selected_phrases,
                                    existing_operators_json=json.dumps(existing_operators),
                                    existing_phrases_json=json.dumps(existing_phrases)
                                    )
    
    return app


if __name__ == '__main__':
    app = create_app()

    static_folder = r"C:\Users\Jack\Documents\Code\FeedbackSummarySentiment\app\static"
    extra_files = [os.path.join(static_folder, f) for f in os.listdir(static_folder)]

    app.run(debug=True, extra_files=extra_files, host="0.0.0.0")

    




