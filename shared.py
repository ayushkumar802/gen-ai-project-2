# shared.py
from mydb import AsyncDatabaseManager
from rediss import CacheManager

db    = AsyncDatabaseManager()
cache = CacheManager()