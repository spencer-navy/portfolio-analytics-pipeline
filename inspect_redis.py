"""
Redis Cache Inspector

This script connects to Redis and displays all the cached metrics
from your analytics pipeline.

Usage:
    python inspect_redis.py
"""

import os
import redis
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables
load_dotenv('.env.local')

REDIS_URL = os.getenv('REDIS_URL')

if not REDIS_URL:
    print("❌ REDIS_URL not set in environment")
    exit(1)

# Connect to Redis
print("Connecting to Redis...")
r = redis.from_url(REDIS_URL, decode_responses=True)

try:
    r.ping()
    print("✅ Connected to Redis\n")
except redis.ConnectionError as e:
    print(f"❌ Failed to connect to Redis: {e}")
    exit(1)

print("=" * 70)
print("REDIS CACHE CONTENTS")
print("=" * 70)

# 1. Total Events
print("\n📊 TOTAL EVENTS:")
total = r.get('total_events')
print(f"   Total events processed: {total}")

# 2. Event Counts by Type
print("\n📈 EVENT COUNTS BY TYPE:")
event_counts = r.hgetall('event_counts')
if event_counts:
    for event_type, count in sorted(event_counts.items(), key=lambda x: int(x[1]), reverse=True):
        print(f"   {event_type}: {count}")
else:
    print("   (No event counts found)")

# 3. Session Statistics
print("\n👥 SESSION STATISTICS:")
total_sessions = r.get('total_sessions')
avg_duration = r.get('avg_session_duration')
avg_events = r.get('avg_events_per_session')
print(f"   Total sessions: {total_sessions}")
print(f"   Average session duration: {avg_duration}s")
print(f"   Average events per session: {avg_events}")

# 4. Device Distribution
print("\n📱 DEVICE DISTRIBUTION:")
devices = r.hgetall('device_distribution')
if devices:
    for device, count in sorted(devices.items(), key=lambda x: int(x[1]), reverse=True):
        print(f"   {device}: {count}")
else:
    print("   (No device data found)")

# 5. Popular Pages (Top 10)
print("\n📄 TOP 10 POPULAR PAGES:")
popular_pages = r.zrevrange('popular_pages', 0, 9, withscores=True)
if popular_pages:
    for i, (page, score) in enumerate(popular_pages, 1):
        print(f"   {i}. {page}: {int(score)} views")
else:
    print("   (No page data found)")

# 6. Popular Filters (Top 10)
print("\n🔍 TOP 10 POPULAR FILTERS:")
popular_filters = r.zrevrange('popular_filters', 0, 9, withscores=True)
if popular_filters:
    for i, (filter_id, score) in enumerate(popular_filters, 1):
        print(f"   {i}. {filter_id}: {int(score)} clicks")
else:
    print("   (No filter data found)")

# 7. Popular Projects (Top 10)
print("\n🎯 TOP 10 POPULAR PROJECTS:")
popular_projects = r.zrevrange('popular_projects', 0, 9, withscores=True)
if popular_projects:
    for i, (project_id, score) in enumerate(popular_projects, 1):
        print(f"   {i}. {project_id}: {int(score)} clicks")
else:
    print("   (No project data found)")

# 8. Events Per Minute (Last 10)
print("\n⏱️  RECENT EVENTS PER MINUTE (Last 10):")
events_per_min = r.zrevrange('events_per_minute', 0, 9, withscores=True)
if events_per_min:
    for member, score in events_per_min:
        # Split on last colon only (ISO timestamps have multiple colons)
        timestamp, count = member.rsplit(':', 1)
        dt = datetime.fromisoformat(timestamp)
        print(f"   {dt.strftime('%Y-%m-%d %H:%M')}: {count} events")
else:
    print("   (No time series data found)")

# 9. Last Cache Update
print("\n🕐 CACHE METADATA:")
last_update = r.get('last_cache_update')
if last_update:
    dt = datetime.fromisoformat(last_update)
    print(f"   Last cache update: {dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
else:
    print("   (No update timestamp found)")

# 10. All Keys in Redis
print("\n🔑 ALL REDIS KEYS:")
all_keys = r.keys('*')
print(f"   Total keys: {len(all_keys)}")
if len(all_keys) <= 20:
    for key in sorted(all_keys):
        key_type = r.type(key)
        print(f"   - {key} ({key_type})")
else:
    print(f"   (Too many keys to display - {len(all_keys)} total)")

print("\n" + "=" * 70)
print("✅ Inspection complete!")
print("=" * 70)