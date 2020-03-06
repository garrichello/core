""" Debug script """

import time
import argparse
import sys

import ptvsd

sys.path.insert(0, './source')
import run_core

ptvsd.enable_attach(address=('localhost', 3001))

#Enable the below line of code only if you want the application to wait untill the debugger has attached to it
ptvsd.wait_for_attach()

#Let's sleep a second to synchronize with remote debugger
time.sleep(1)

#Let's create an argument parser and feed it with a fake argument, name of the simple task file
parser = argparse.ArgumentParser(description="Computing and Visualizing Core")
parser.add_argument("task_file_name", help="name of an XML task file")
args = parser.parse_args(['./debug/debug.tmpl.xml'])

#It starts from the main function of the Core
run_core.main(args)

