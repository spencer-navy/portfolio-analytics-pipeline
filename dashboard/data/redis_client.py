"""
Redis Data Fetcher for Analytics Dashboard

Fetches pre-calculated metrics from Redis cache for instant dashboard loading.
"""

import os
import redis
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

load_dotenv('.env.local')

class RedisDataFetcher:
    """
    Connects to Redis and provides methods to fetch dashboard metrics.
    All data is pre-calculated by batch_processor.py.
    """
    
    def __init__(self, redis_url: str = None):
        """Initialize Redis connection."""
        self.redis_url = redis_url or os.getenv('REDIS_URL')
        
        if not self.redis_url:
            raise ValueError("REDIS_URL not set in environment")
        
        self.client = redis.from_url(self.redis_url, decode_responses=True)

        # Test connection
        try:
            self.client.ping()
            print("Connected to Redis")
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            raise

    def get_total_events(self) -> int:
        """Get total number of processed events."""
        total = self.client.get('total_events')
        return int(total) if total else 0
    
    def get_event_counts(self) -> Dict[str, int]:
        """Get event counts by type."""
        counts = self.client.hgetall('event_counts')
        return {k: int(v) for k, v in counts.items()} if counts else {}
    
    def get_session_stats(self) -> Dict[str, any]:
        """Get session statistics."""
        return {
            'total_sessions': int(self.client.get('total_sessions') or 0),
            'avg_duration': float(self.client.get('avg_session_duration') or 0),
            'avg_events_per_session': float(self.client.get('avg_events_per_session') or 0)
        }
    
    def get_device_distribution(self) -> Dict[str, int]:
        """Get device type distribution."""
        devices = self.client.hgetall('device_distribution')
        return {k: int(v) for k, v in devices.items()} if devices else {}
    
    def get_popular_pages(self, limit: int = 10) -> List[Tuple[str, int]]:
        """Get top N popular pages with view counts."""
        pages = self.client.zrevrange('popular_pages', 0, limit - 1, withscores=True)
        return [(page, int(score)) for page, score in pages] if pages else []
    
    def get_events_per_minute(self, limit: int = 60) -> List[Dict[str, any]]:
        """Get events per minute time series (last N minutes)."""
        events = self.client.zrevrange('events_per_minute', 0, limit - 1, withscores=True)
        
        result = []
        for member, score in events:
            # Parse "timestamp:count" format
            timestamp_str, count = member.rsplit(':', 1)
            result.append({
                'timestamp': timestamp_str,
                'count': int(count),
                'datetime': datetime.fromisoformat(timestamp_str)
            })
        
        # Return in chronological order
        return sorted(result, key=lambda x: x['datetime'])
    
    def get_all_dashboard_data(self) -> Dict:
        """
        Get all dashboard data in one call for efficiency.
        Returns a dictionary with all metrics.
        """
        return {
            'total_events': self.get_total_events(),
            'event_counts': self.get_event_counts(),
            'session_stats': self.get_session_stats(),
            'device_distribution': self.get_device_distribution(),
            'popular_pages': self.get_popular_pages(),
            'popular_filters': self.get_popular_filters(),
            'popular_projects': self.get_popular_projects(),
            'events_timeline': self.get_events_per_minute(),
            'last_update': self.get_last_update()
        }
    
    def close(self):
        """Close Redis connection."""
        self.client.close()

if __name__ == '__main__':
    fetcher = RedisDataFetcher()
    data = fetcher.get_all_dashboard_data()
    
    print("\n📊 Dashboard Data Summary:")
    print(f"Total Events: {data['total_events']}")
    print(f"Total Sessions: {data['session_stats']['total_sessions']}")
    print(f"Average Session: {data['session_stats']['avg_duration']:.1f}s")
    print(f"\nTop 3 Pages:")
    for page, count in data['popular_pages'][:3]:
        print(f"  {page}: {count} views")
    
    fetcher.close()