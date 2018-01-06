# Data Module

Contains all classes and functions required to run a DataNode.

A DataNode consists of a API Interface (Websocket connection), a Publisher
object (which allows other processes to subscribe to its data) and
a data manager, which keeps track of the various data mined
(current order book, last X trades, last Y candles).