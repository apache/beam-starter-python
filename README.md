# ato-demo-2022

To run on Dataflow:

```
python3 -m venv ~/.virtualenvs/env
. ~/.virtualenvs/env/bin/activate

pip install -e .
python main.py --region us-central1 --runner DataflowRunner --project <your project> --temp_location gs://path/to/temp/location --streaming --use_runner_v2 --allow_unsafe_triggers
```