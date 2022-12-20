#!/bin/python3
import argparse

from Archive_utils import *

## ArgParser
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--env", help="Environment to use i.e, prod, dev or test")
args = parser.parse_args()


if args.env is not None:
    archiveFolderData(args.env)
else:
    archiveFolderData()