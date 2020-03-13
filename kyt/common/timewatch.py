# Copyright (C) 2020 You-Cast on Earth, Moon and Mars 2020
# This file is part of com.castsoftware.uc.kyt extension
# which is released under GNU GENERAL PUBLIC LICENS v3, Version 3, 29 June 2007.
# See file LICENCE or go to https://www.gnu.org/licenses/ for full license details.

import time
import datetime


class TimeWatch:
    __slots__ = '_elapsedStart', '_elapsedStop', '_cpuStart', '_cpuStop'

    def __init__( self, aStart=False ):
        self._elapsedStart = self._elapsedStop = time.perf_counter()
        self._cpuStart = self._cpuStop = time.process_time()

    def start( self ):
        self._elapsedStop = self._elapsedStart = time.perf_counter()
        self._cpuStop = self._cpuStart = time.process_time()

    def stop( self ):
        self._elapsedStop = time.perf_counter()
        self._cpuStop = time.process_time()
        return self.deltaElapsed()

    def deltaElapsed( self ):
        return ( self._elapsedStop - self._elapsedStart )

    def deltaCpu( self ):
        return ( self._cpuStop - self._cpuStart )

    def generateDateTimeMSecStamp():
        vDT = datetime.datetime.now()
        return "{:0>4}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2},{:0>3}".format(
                vDT.year, vDT.month, vDT.day,
                vDT.hour, vDT.minute, vDT.second, int(vDT.microsecond/1000)
            )
