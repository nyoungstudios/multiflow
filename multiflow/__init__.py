"""
Multiflow Python Package

Source code: https://github.com/nyoungstudios/multiflow

Copyright (c) 2021 Nathaniel Young

Distributed under the MIT License
"""
__author__ = 'Nathaniel Young'

from .version import __version__
from .thread import MultithreadedGeneratorBase, MultithreadedGenerator, MultithreadedFlow, JobOutput, \
    FlowException

__all__ = ['MultithreadedGeneratorBase', 'MultithreadedGenerator', 'MultithreadedFlow', 'JobOutput', 'FlowException']
