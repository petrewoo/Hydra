#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import yaml
import yamlordereddictloader


def load_config(file):
    with open(file, 'r') as f:
        return yaml.load(f, Loader=yamlordereddictloader.Loader)
