#!/bin/sh

PYTHONASYNCIODEBUG=0 PYTHONPATH=`pwd` py.test tests
PYTHONASYNCIODEBUG=1 PYTHONPATH=`pwd` py.test tests 
