# Predicting Unseen Process Behavior Based on Log Injection

## Description
This repository contains data and code for the article ''Predicting Unseen Process Behavior Based on Log Injection''

## Getting started

Create a virtual environment
```
python3 -m venv venv
```

Activate your virtual environment
```
source venv/bin/activate
```

Install dependencies
```
pip install -r requirements.txt
```

Augment event log based on the associated constraint
```
python3 augment_log.py --dataset=BPI12w_C1
```

## License
LGPL-3.0 license
