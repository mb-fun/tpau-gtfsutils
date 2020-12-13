# from enum import Enum

class GTFSExceptionType:
    ADDED = '1'
    REMOVED = '2'

# This is silly, but allows an easy change to int or bool values if needed later
class GTFSBool:
    TRUE = '1'
    FALSE = '0'