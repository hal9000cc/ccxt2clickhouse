"""
CCXT to ClickHouse
Package for loading cryptocurrency quotes from exchanges via CCXT into ClickHouse
and accessing the data from ClickHouse.
"""

__version__ = '0.1.0'

from .exceptions import C2CException, C2CExceptionBadTimeframeValue
from .timeframe import Timeframe

__all__ = [
    'C2CException',
    'C2CExceptionBadTimeframeValue',
    'Timeframe',
    '__version__',
]

