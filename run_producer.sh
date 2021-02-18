python -c 'import sys; exit(1) if sys.version_info.major < 3 else exit(0)'
 
if [[ $? == 0 ]]; then
    [ ! -d "venv" ] && virtualenv -p python venv
    .venv/bin/activate
    pip install -r requirements.txt
else
    [ ! -d "venv" ] && virtualenv -p python3 venv
    .venv/bin/activate
    pip3 install -r requirements.txt
fi
 
python ./kafka_producer/producer.py