#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os

_root = os.path.abspath(os.path.dirname(__file__))

CONF_FILE = _root + '/' + 'conf/zkServerConf.yaml'
INIT_FILE = _root + '/' + 'conf/zkInitConf.yaml'
HIST_FILE = _root + '/' + 'logs/cliCmdHist.txt'
