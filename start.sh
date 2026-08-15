#!/bin/bash

source ./.env
PYTHON=/warehouse/GitRepos/trading-engine-platform/.trading/bin/python

#nohup $PYTHON -m trading_engine position > nohup-position.log 2>&1 &
#nohup $PYTHON -m trading_engine strategy --stream --interval-seconds 1 > nohup-strategy.log 2>&1 &
nohup $PYTHON -m trading_engine risk > nohup-risk.log 2>&1 &
#nohup $PYTHON -m trading_engine position-projector -- --stream --interval-seconds 1 > nohup-projector.log 2>&1 &
