#!/bin/bash
source /home/hit/.virtualenvs/分类分级/bin/activate
export PYTHONPATH="$PYTHONPATH:/home/hit/DataMasking/MakingService"
python ./service/desensitization_params.py
python app.py
