"""
Batch Event Processor for Portfolio Analytics Pipeline

This script connects to MongoDB, loads event data, cleans it, performs feature engineering,
and updates Redis cache with pre-aggregated metrics for the dashboard.

Author: Abigail Spencer
Created: January 2026
Purpose: Demonstrate production-grade data pipeline for ML Engineering portfolio

ARCHITECTURE:
    MongoDB (raw events) → This Script (clean & aggregate) → Redis (cached metrics)
                                                            → S3 (optional, cleaned data)
"""

import os
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import redis
from urllib.parse import urlparse
import hashlib
import logging
import json

# Load environment variables from .env.local file (if it exists - for local development)
load_dotenv('.env.local')

# Configure logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    Processes event data from MongoDB in hourly batches.
    
    This class handles:
    1. Connecting to MongoDB and Redis
    2. Loading events from a specific time range
    3. Cleaning and transforming the data
    4. Feature engineering (creating new useful columns)
    5. Updating Redis cache with aggregated metrics
    6. Optionally saving cleaned data to files
    
    Design decisions:
    - Uses pandas for data manipulation (industry standard for data processing)
    - Implements error handling for database connections
    - Creates reusable methods for different processing steps
    - Follows clean code principles (single responsibility, clear naming)
    """
    
    def __init__(self, mongo_uri: str = None, redis_url: str = None):
        """
        Initialize the batch processor with database connections.
        
        Args:
            mongo_uri (str): MongoDB connection string. If None, reads from environment
            redis_url (str): Redis connection string. If None, reads from environment
            
        Environment Variables Expected:
            MONGODB_URI: MongoDB Atlas connection string (e.g., mongodb+srv://user:pass@cluster.mongodb.net/db)
            REDIS_URL: Redis connection string (e.g., redis://localhost:6379)
        
        Example:
            # Using environment variables
            processor = BatchProcessor()
            
            # Explicit connection strings
            processor = BatchProcessor(
                mongo_uri="mongodb://localhost:27017",
                redis_url="redis://localhost:6379"
            )
        """
        # Get connection strings from parameters or environment variables
        # The 'or' operator returns the first truthy value
        self.mongo_uri = mongo_uri or os.getenv('MONGODB_URI')
        self.redis_url = redis_url or os.getenv('REDIS_URL')  # None if not set
        
        # Validate that we have required connection strings
        if not self.mongo_uri:
            raise ValueError(
                "MongoDB URI is required. Set MONGODB_URI environment variable or pass mongo_uri parameter."
            )
        
        # Initialize database connections
        self.mongo_client = None
        self.redis_client = None
        self.db = None
        self.events_collection = None
        
        # Connect to databases (separate method for better error handling)
        self._connect_to_databases()
        
        logger.info("BatchProcessor initialized successfully")
    
    def _connect_to_databases(self):
        """
        Establish connections to MongoDB and Redis.
        
        This is a private method (indicated by leading underscore) that handles
        database connection logic separately from initialization.
        
        Raises:
            ConnectionFailure: If MongoDB connection fails
            redis.ConnectionError: If Redis connection fails
        """
        try:
            # Connect to MongoDB
            # serverSelectionTimeoutMS: How long to wait for connection before failing
            logger.info("Connecting to MongoDB...")
            self.mongo_client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )
            
            # Test the connection by running a simple command
            # This forces the connection to actually establish (MongoClient is lazy)
            self.mongo_client.admin.command('ping')
            
            # Get database and collection references
            # 'analytics' is the database name, 'events' is the collection
            self.db = self.mongo_client['analytics']
            self.events_collection = self.db['events']
            
            logger.info(f"✓ Connected to MongoDB (Database: analytics)")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"✗ Failed to connect to MongoDB: {e}")
            raise
        
        # Only try to connect to Redis if URL is provided
        if self.redis_url:
            try:
                # Connect to Redis
                # decode_responses=True means Redis returns strings instead of bytes
                logger.info("Connecting to Redis...")
                self.redis_client = redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=5  # 5 second timeout
                )
                
                # Test the connection
                self.redis_client.ping()
                logger.info("✓ Connected to Redis")
                
            except redis.ConnectionError as e:
                logger.error(f"✗ Failed to connect to Redis: {e}")
                logger.warning("Continuing without Redis - caching will be disabled")
                self.redis_client = None
        else:
            logger.info("Redis URL not provided - caching will be disabled")
            self.redis_client = None
    
    def load_events(
        self, 
        start_date: datetime = None, 
        end_date: datetime = None,
        event_types: List[str] = None
    ) -> pd.DataFrame:
        """
        Load events from MongoDB within a specified time range.
        
        This method:
        1. Builds a MongoDB query based on time range and event types
        2. Executes the query and retrieves documents
        3. Converts MongoDB documents (BSON) to pandas DataFrame
        4. Performs initial data type conversions
        
        Args:
            start_date (datetime): Start of time range (inclusive). If None, loads all events
            end_date (datetime): End of time range (inclusive). If None, uses current time
            event_types (List[str]): Filter by specific event types (e.g., ['filter_click', 'project_click'])
        
        Returns:
            pd.DataFrame: Events with columns matching MongoDB schema
        
        Example:
            # Load last hour of events
            from zoneinfo import ZoneInfo
            est = ZoneInfo('America/New_York')
            start = datetime.now(est) - timedelta(hours=1)
            end = datetime.now(est)
            df = processor.load_events(start_date=start, end_date=end)
            
            # Load only filter clicks from last 24 hours
            start = datetime.now(est) - timedelta(days=1)
            df = processor.load_events(start_date=start, event_types=['filter_click'])
        """
        logger.info("Loading events from MongoDB...")
        
        # Build MongoDB query dictionary
        query = {}
        
        # Add time range filter if provided
        if start_date or end_date:
            # MongoDB uses '$gte' (greater than or equal) and '$lte' (less than or equal)
            query['timestamp'] = {}
            if start_date:
                query['timestamp']['$gte'] = start_date
                logger.info(f"  Filtering: timestamp >= {start_date}")
            if end_date:
                query['timestamp']['$lte'] = end_date
                logger.info(f"  Filtering: timestamp <= {end_date}")
        
        # Add event type filter if provided
        if event_types:
            # MongoDB uses '$in' operator for "match any of these values"
            query['eventType'] = {'$in': event_types}
            logger.info(f"  Filtering: eventType in {event_types}")
        
        try:
            # Execute query and convert to list of dictionaries
            # .find() returns a cursor (lazy iterator), list() forces it to load all documents
            events = list(self.events_collection.find(query))
            
            logger.info(f"✓ Loaded {len(events)} events from MongoDB")
            
            # Handle empty result
            if not events:
                logger.warning("No events found matching criteria")
                return pd.DataFrame()  # Return empty DataFrame
            
            # Convert list of dictionaries to pandas DataFrame
            df = pd.DataFrame(events)
            
            # MongoDB's ObjectId type doesn't work well in pandas, convert to string
            if '_id' in df.columns:
                df['_id'] = df['_id'].astype(str)
            
            # Convert timestamp to pandas datetime for easier manipulation
            # MongoDB stores timestamps as datetime objects, but pandas has more features
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            logger.info(f"✓ Converted to DataFrame with shape {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"✗ Error loading events: {e}")
            raise
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform the raw event data.
        
        Data cleaning steps:
        1. Handle missing values (fill with defaults or drop)
        2. Remove duplicate events (same session, event, time)
        3. Standardize text fields (lowercase, strip whitespace)
        4. Extract components from complex fields (URLs, user agents)
        5. Add derived time-based features (hour, day of week)
        6. Flatten nested metadata into separate columns
        
        Args:
            df (pd.DataFrame): Raw events from MongoDB
        
        Returns:
            pd.DataFrame: Cleaned and transformed events
        
        Design rationale:
        - Creates a copy to avoid modifying original data
        - Each step is modular and could be extracted to separate methods
        - Handles edge cases (empty dataframe, missing columns)
        """
        if df.empty:
            logger.warning("Cannot clean empty DataFrame")
            return df
        
        logger.info("Cleaning DataFrame...")
        
        # Create a copy to avoid modifying the original
        # This is important for debugging and data lineage
        cleaned = df.copy()
        
        # === STEP 1: Handle Missing Values ===
        logger.info("  Step 1: Handling missing values")
        
        # Referrer is often null (direct traffic), replace with 'direct'
        if 'referrer' in cleaned.columns:
            cleaned['referrer'] = cleaned['referrer'].fillna('direct')
            # Also clean up empty strings
            cleaned['referrer'] = cleaned['referrer'].replace('', 'direct')
        
        # Page should never be null, but if it is, drop those rows
        if 'page' in cleaned.columns:
            before_count = len(cleaned)
            null_page_events = cleaned[cleaned['page'].isna()].copy()
            
            if len(null_page_events) > 0:
                logger.warning(f"  Found {len(null_page_events)} events with null page values:")
                for idx, row in null_page_events.head(5).iterrows():
                    logger.warning(f"    - Event {row.get('_id')}: {row.get('eventType')} at {row.get('timestamp')}")
                if len(null_page_events) > 5:
                    logger.warning(f"    ... and {len(null_page_events) - 5} more")
                
                # Save removed events to MongoDB 'removed_events' collection
                removed_collection = self.events_collection.database['removed_events']
                null_page_copy = null_page_events.copy()
                null_page_copy['removal_reason'] = 'null_page_value'
                null_page_copy['removed_at'] = datetime.now(ZoneInfo('America/New_York'))
                
                # Convert to JSON and back to handle pandas types and nested objects properly
                json_str = null_page_copy.to_json(orient='records', date_format='iso')
                null_page_records = json.loads(json_str)
                
                # Re-add the removal metadata (lost in JSON conversion)
                for record in null_page_records:
                    record['removal_reason'] = 'null_page_value'
                    record['removed_at'] = datetime.now(ZoneInfo('America/New_York'))
                
                removed_collection.insert_many(null_page_records)
                logger.info(f"  Saved {len(null_page_events)} null-page events to 'removed_events' collection")
            
            cleaned = cleaned.dropna(subset=['page'])
            dropped = before_count - len(cleaned)
            if dropped > 0:
                logger.warning(f"  Dropped {dropped} rows with null page values")
        
        # === STEP 2: Remove Duplicates ===
        logger.info("  Step 2: Removing duplicates")

        # Sort by timestamp to keep the first occurrence
        cleaned = cleaned.sort_values('timestamp')

        # Add rounded timestamp for duplicate detection
        cleaned['timestamp_rounded'] = cleaned['timestamp'].dt.floor('1S')

        # Define what makes an event "duplicate"
        # Same session, same event type, same page, same second = likely duplicate
        duplicate_subset = ['sessionId', 'eventType', 'page', 'timestamp_rounded']

        before_count = len(cleaned)

        # Identify duplicates before removing them
        duplicate_mask = cleaned.duplicated(subset=duplicate_subset, keep='first')
        duplicate_events = cleaned[duplicate_mask].copy()

        if len(duplicate_events) > 0:
            logger.warning(f"  Found {len(duplicate_events)} duplicate events:")
            
            # Show breakdown by event type
            dup_by_type = duplicate_events['eventType'].value_counts()
            for event_type, count in dup_by_type.items():
                logger.warning(f"    - {event_type}: {count} duplicates")
            
            # Save duplicate events to MongoDB 'removed_events' collection
            removed_collection = self.events_collection.database['removed_events']
            duplicate_events_copy = duplicate_events.drop('timestamp_rounded', axis=1).copy()
            duplicate_events_copy['removal_reason'] = 'duplicate_event'
            duplicate_events_copy['removed_at'] = datetime.now(ZoneInfo('America/New_York'))
            
            # Convert to JSON and back to handle pandas types
            json_str = duplicate_events_copy.to_json(orient='records', date_format='iso')
            duplicate_records = json.loads(json_str)
            
            # Re-add metadata
            for record in duplicate_records:
                record['removal_reason'] = 'duplicate_event'
                record['removed_at'] = datetime.now(ZoneInfo('America/New_York'))
            
            if duplicate_records:
                try:
                    removed_collection.insert_many(duplicate_records, ordered=False)
                    logger.info(f"  Saved {len(duplicate_events)} duplicate events to 'removed_events' collection")
                except Exception as e:
                    # Some might already exist, that's okay
                    logger.info(f"  Attempted to save duplicates (some may already exist)")

        # Remove duplicates
        cleaned = cleaned.drop_duplicates(subset=duplicate_subset, keep='first')

        # Drop the helper column
        cleaned = cleaned.drop('timestamp_rounded', axis=1)

        duplicates_removed = before_count - len(cleaned)

        if duplicates_removed > 0:
            logger.info(f"  Removed {duplicates_removed} duplicate events")

        
        # === STEP 3: Extract Time-Based Features ===
        logger.info("  Step 3: Creating time-based features")
        
        if 'timestamp' in cleaned.columns:
            # Extract date without time (useful for daily aggregations)
            cleaned['date'] = cleaned['timestamp'].dt.date
            
            # Extract hour of day (0-23) for hourly patterns
            cleaned['hour'] = cleaned['timestamp'].dt.hour
            
            # Day of week as string (Monday, Tuesday, etc.)
            cleaned['day_of_week'] = cleaned['timestamp'].dt.day_name()
            
            # Numeric day of week (0=Monday, 6=Sunday) for sorting/grouping
            cleaned['day_of_week_num'] = cleaned['timestamp'].dt.dayofweek
            
            # Is it a weekend? (useful for traffic pattern analysis)
            cleaned['is_weekend'] = cleaned['day_of_week_num'].isin([5, 6])
            
            logger.info(f"  Added time features: date, hour, day_of_week, is_weekend")
        
        # === STEP 4: Parse and Extract Domain from Referrer ===
        logger.info("  Step 4: Extracting referrer domain")
        
        if 'referrer' in cleaned.columns:
            cleaned['referrer_domain'] = cleaned['referrer'].apply(self._extract_domain)
            logger.info(f"  Extracted {cleaned['referrer_domain'].nunique()} unique referrer domains")
        
        # === STEP 5: Parse User Agent (Browser/Device Info) ===
        logger.info("  Step 5: Parsing user agents")
        
        if 'userAgent' in cleaned.columns:
            # This is a simplified version - in production you'd use a library like user-agents
            cleaned['device_type'] = cleaned['userAgent'].apply(self._parse_device_type)
            cleaned['browser'] = cleaned['userAgent'].apply(self._parse_browser)
            logger.info(f"  Detected devices: {cleaned['device_type'].value_counts().to_dict()}")
        
        # === STEP 6: Hash IP Addresses for Privacy ===
        logger.info("  Step 6: Hashing IP addresses for privacy")
        
        if 'ipAddress' in cleaned.columns:
            # Convert IP to hash (can't reverse engineer back to original IP)
            # This is GDPR-friendly while still allowing unique visitor counting
            cleaned['ip_hash'] = cleaned['ipAddress'].apply(self._hash_ip)
            
            # Drop the original IP address to avoid storing PII
            cleaned = cleaned.drop('ipAddress', axis=1)
            logger.info("  IP addresses hashed and original IPs removed")
        
        # === STEP 7: Flatten Nested Metadata ===
        logger.info("  Step 7: Flattening metadata object")
        
        if 'metadata' in cleaned.columns:
            # Metadata is a nested dictionary, flatten it into separate columns
            # Example: {'filterValue': 'Python'} becomes a column 'filterValue'
            
            # Check if metadata column actually has data
            if cleaned['metadata'].notna().any():
                # pd.json_normalize flattens nested JSON into columns
                metadata_df = pd.json_normalize(cleaned['metadata'])
                
                # Prefix metadata columns to avoid name conflicts
                # 'filterValue' becomes 'meta_filterValue'
                metadata_df.columns = ['meta_' + col for col in metadata_df.columns]
                
                # Combine with main dataframe
                # axis=1 means concatenate columns (side by side)
                cleaned = pd.concat(
                    [cleaned.drop('metadata', axis=1), metadata_df],
                    axis=1
                )
                
                logger.info(f"  Flattened metadata into {len(metadata_df.columns)} columns")
            else:
                # No metadata to flatten, just drop the column
                cleaned = cleaned.drop('metadata', axis=1)
        
        logger.info(f"✓ Cleaning complete. Final shape: {cleaned.shape}")
        return cleaned
    
    def add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer additional features for analytics and machine learning.
        
        Feature engineering creates new columns that make patterns more obvious:
        1. Session-level metrics (duration, event count)
        2. Time since previous event (engagement indicator)
        3. Sequential event patterns (page flow)
        4. User behavior flags (quick exit, deep engagement)
        
        These features are useful for:
        - Dashboard visualizations
        - Anomaly detection
        - Future recommendation engine
        - User segmentation
        
        Args:
            df (pd.DataFrame): Cleaned events DataFrame
        
        Returns:
            pd.DataFrame: Events with additional engineered features
        """
        if df.empty:
            return df
        
        logger.info("Engineering features...")
        
        # Create a copy to avoid modifying original
        featured = df.copy()
        
        # === SESSION-LEVEL FEATURES ===
        logger.info("  Calculating session-level metrics")
        
        # For each session, calculate aggregate statistics
        session_stats = featured.groupby('sessionId').agg({
            'timestamp': ['min', 'max', 'count'],  # First event, last event, total events
            'eventType': lambda x: x.nunique(),     # Number of unique event types
            'page': lambda x: x.nunique()           # Number of unique pages visited
        })
        
        # Flatten multi-level column names
        # ('timestamp', 'min') becomes 'session_start'
        session_stats.columns = [
            'session_start',
            'session_end', 
            'event_count',
            'unique_event_types',
            'unique_pages'
        ]
        
        # Calculate session duration in seconds
        session_stats['session_duration_seconds'] = (
            session_stats['session_end'] - session_stats['session_start']
        ).dt.total_seconds()
        
        # Merge session stats back to main dataframe
        # This adds the same session metrics to every event in that session
        featured = featured.merge(
            session_stats,
            left_on='sessionId',
            right_index=True,  # session_stats has sessionId as index
            how='left'
        )
        
        logger.info(f"  Added session features: duration, event_count, unique_pages")
        
        # === TIME SINCE PREVIOUS EVENT ===
        logger.info("  Calculating time between events")
        
        # Sort by session and time to ensure correct order
        featured = featured.sort_values(['sessionId', 'timestamp'])
        
        # Calculate time difference from previous event in the same session
        # .diff() subtracts the previous row's value
        # .groupby() ensures we only compare events within the same session
        featured['seconds_since_last_event'] = (
            featured.groupby('sessionId')['timestamp']
            .diff()
            .dt.total_seconds()
        )
        
        # First event in a session will have NaN, fill with 0
        featured['seconds_since_last_event'] = (
            featured['seconds_since_last_event'].fillna(0)
        )
        
        # === ENGAGEMENT FLAGS ===
        logger.info("  Creating engagement indicators")
        
        # Quick exit: less than 10 seconds on site
        featured['is_quick_exit'] = featured['session_duration_seconds'] < 10
        
        # Engaged session: more than 30 seconds or 3+ events
        featured['is_engaged'] = (
            (featured['session_duration_seconds'] > 30) | 
            (featured['event_count'] > 3)
        )
        
        # Deep engagement: more than 2 minutes or 5+ events
        featured['is_deep_engagement'] = (
            (featured['session_duration_seconds'] > 120) | 
            (featured['event_count'] > 5)
        )
        
        # === EVENT SEQUENCE POSITION ===
        logger.info("  Calculating event positions in session")
        
        # Number each event within its session (1st event, 2nd event, etc.)
        featured['event_position_in_session'] = (
            featured.groupby('sessionId').cumcount() + 1
        )
        
        # Is this the first event in the session? (entry point)
        featured['is_entry_event'] = featured['event_position_in_session'] == 1
        
        # Is this the last event? (exit point)
        featured['is_exit_event'] = (
            featured['event_position_in_session'] == featured['event_count']
        )
        
        logger.info(f"✓ Feature engineering complete. Added {len(featured.columns) - len(df.columns)} new features")
        return featured
    
    def update_redis_cache(self, df: pd.DataFrame):
        """
        Update Redis with pre-aggregated metrics for fast dashboard queries.
        
        Instead of the dashboard querying MongoDB every time (slow), we pre-calculate
        metrics and store them in Redis (fast). The dashboard just reads from Redis.
        
        Metrics stored in Redis:
        1. Total event counts by type
        2. Events per minute (time series)
        3. Popular filters (sorted by click count)
        4. Popular projects (sorted by click count)
        5. Popular pages (sorted by view count)
        6. Session statistics (average duration, etc.)
        7. Device type distribution
        
        Redis data structures used:
        - Hash (key-value pairs): For simple counts
        - Sorted Set (scored items): For rankings (top 10 filters, etc.)
        - String (single values): For totals and averages
        
        Args:
            df (pd.DataFrame): Cleaned and featured events DataFrame
        """
        if df.empty or self.redis_client is None:
            logger.warning("Skipping Redis cache update (no data or no Redis connection)")
            return
        
        logger.info("Updating Redis cache...")
        
        try:
            # === METRIC 1: Total Events by Type ===
            logger.info("  Updating event type counts")
            
            # Count events by type
            event_counts = df['eventType'].value_counts()
            
            # Store in Redis Hash: 'event_counts' -> {filter_click: 150, project_click: 89, ...}
            for event_type, count in event_counts.items():
                self.redis_client.hset('event_counts', event_type, int(count))
            
            # Also store total
            self.redis_client.set('total_events', len(df))
            
            # === METRIC 2: Events Per Minute (Time Series) ===
            logger.info("  Updating events per minute time series")
            
            if 'timestamp' in df.columns:
                # Resample to 1-minute buckets and count events
                df_sorted = df.set_index('timestamp').sort_index()
                events_per_minute = df_sorted.resample('1Min').size()
                
                # Store last 60 minutes in Redis (for live chart)
                # Use sorted set with timestamp as score for chronological ordering
                for timestamp, count in events_per_minute.tail(60).items():
                    # Convert timestamp to Unix epoch (seconds since 1970)
                    score = timestamp.timestamp()
                    # Value is "timestamp:count"
                    member = f"{timestamp.isoformat()}:{count}"
                    
                    self.redis_client.zadd(
                        'events_per_minute',
                        {member: score}
                    )
                
                # Keep only last 60 entries to save memory
                # Remove all entries except top 60 (most recent)
                self.redis_client.zremrangebyrank('events_per_minute', 0, -61)
            
            # === METRIC 3: Popular Filters ===
            logger.info("  Updating popular filters")
            
            # Filter clicks have 'meta_filterId' and 'meta_filterValue'
            filter_clicks = df[df['eventType'] == 'filter_click']
            
            if len(filter_clicks) > 0 and 'meta_filterId' in filter_clicks.columns:
                # Count clicks per filter
                filter_counts = filter_clicks['meta_filterId'].value_counts()
                
                # Store in sorted set (automatically sorted by score)
                # Higher score = more popular
                for filter_id, count in filter_counts.items():
                    self.redis_client.zadd(
                        'popular_filters',
                        {filter_id: int(count)}
                    )
                
                logger.info(f"  Tracked {len(filter_counts)} different filters")
            
            # === METRIC 4: Popular Projects ===
            logger.info("  Updating popular projects")
            
            project_clicks = df[df['eventType'] == 'project_click']
            
            if len(project_clicks) > 0 and 'meta_projectId' in project_clicks.columns:
                project_counts = project_clicks['meta_projectId'].value_counts()
                
                for project_id, count in project_counts.items():
                    self.redis_client.zadd(
                        'popular_projects',
                        {project_id: int(count)}
                    )
                
                logger.info(f"  Tracked {len(project_counts)} different projects")
            
            # === METRIC 5: Popular Pages ===
            logger.info("  Updating page view counts")
            
            if 'page' in df.columns:
                page_counts = df['page'].value_counts()
                
                for page, count in page_counts.items():
                    self.redis_client.zadd(
                        'popular_pages',
                        {page: int(count)}
                    )
            
            # === METRIC 6: Session Statistics ===
            logger.info("  Calculating session statistics")
            
            if 'session_duration_seconds' in df.columns:
                # Get unique sessions (one row per session)
                unique_sessions = df.drop_duplicates(subset=['sessionId'])
                
                # Calculate averages
                avg_duration = unique_sessions['session_duration_seconds'].mean()
                avg_events_per_session = unique_sessions['event_count'].mean()
                
                # Store in Redis
                self.redis_client.set('avg_session_duration', f"{avg_duration:.2f}")
                self.redis_client.set('avg_events_per_session', f"{avg_events_per_session:.2f}")
                
                # Total unique sessions
                total_sessions = len(unique_sessions)
                self.redis_client.set('total_sessions', total_sessions)
                
                logger.info(f"  Avg session duration: {avg_duration:.1f}s, Events/session: {avg_events_per_session:.1f}")
            
            # === METRIC 7: Device Type Distribution ===
            logger.info("  Updating device type distribution")
            
            if 'device_type' in df.columns:
                device_counts = df['device_type'].value_counts()
                
                for device, count in device_counts.items():
                    self.redis_client.hset('device_distribution', device, int(count))
            
            # === METRIC 8: Set Last Updated Timestamp ===
            # Useful for showing "data as of..." in dashboard
            est = ZoneInfo('America/New_York')
            self.redis_client.set(
                'last_cache_update',
                datetime.now(est).isoformat()
            )
            
            logger.info("✓ Redis cache updated successfully")
            
        except redis.RedisError as e:
            logger.error(f"✗ Redis error: {e}")
            # Don't raise - caching failure shouldn't stop the pipeline
    
    # ========== HELPER METHODS ==========
    # These are utility functions used by the main processing methods
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """
        Extract domain from URL.
        
        Examples:
            'https://google.com/search?q=test' -> 'google.com'
            'direct' -> 'direct'
            '' -> 'direct'
        """
        if not url or url == 'direct' or url == '':
            return 'direct'
        
        try:
            parsed = urlparse(url)
            # netloc is the domain part of URL
            domain = parsed.netloc
            return domain if domain else 'direct'
        except:
            return 'unknown'
    
    @staticmethod
    def _parse_device_type(user_agent: str) -> str:
        """
        Simple device type detection from user agent string.
        
        In production, you'd use a library like `user-agents` for better accuracy.
        This simplified version looks for common keywords.
        
        Returns: 'mobile', 'tablet', or 'desktop'
        """
        if not user_agent:
            return 'unknown'
        
        ua_lower = user_agent.lower()
        
        # Check for mobile indicators
        mobile_keywords = ['mobile', 'android', 'iphone', 'ipod', 'blackberry', 'windows phone']
        if any(keyword in ua_lower for keyword in mobile_keywords):
            return 'mobile'
        
        # Check for tablet indicators
        tablet_keywords = ['ipad', 'tablet', 'kindle']
        if any(keyword in ua_lower for keyword in tablet_keywords):
            return 'tablet'
        
        return 'desktop'
    
    @staticmethod
    def _parse_browser(user_agent: str) -> str:
        """
        Simple browser detection from user agent string.
        
        Returns: 'chrome', 'firefox', 'safari', 'edge', or 'other'
        """
        if not user_agent:
            return 'unknown'
        
        ua_lower = user_agent.lower()
        
        # Check in order of specificity
        # Edge must come before Chrome (Edge contains "chrome" in UA)
        if 'edg' in ua_lower:
            return 'edge'
        elif 'chrome' in ua_lower or 'crios' in ua_lower:
            return 'chrome'
        elif 'firefox' in ua_lower or 'fxios' in ua_lower:
            return 'firefox'
        elif 'safari' in ua_lower:
            return 'safari'
        else:
            return 'other'
    
    @staticmethod
    def _hash_ip(ip: str) -> str:
        """
        Hash IP address for privacy.
        
        Uses SHA-256 hashing so original IP cannot be recovered.
        Truncates to 16 characters to save space.
        
        This is GDPR-compliant while still allowing unique visitor counting.
        
        Example:
            '192.168.1.1' -> '7f83b1657ff1fc53'
        """
        if not ip:
            return 'unknown'
        
        # Create SHA-256 hash
        hash_object = hashlib.sha256(ip.encode())
        # Get hex representation and truncate
        return hash_object.hexdigest()[:16]
    
    def close(self):
        """
        Close database connections gracefully.
        
        Always call this when done processing to free up connections.
        Best practice: use context manager or try/finally block.
        """
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
        
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")


# ========== MAIN EXECUTION ==========
# This code runs when the script is executed directly (not imported)

def main():
    """
    Main entry point for the batch processing job.
    
    This function:
    1. Initializes the processor
    2. Loads events from the last hour
    3. Cleans and transforms the data
    4. Engineers features
    5. Updates Redis cache (if available)
    6. Optionally trains ML models (daily at midnight UTC)
    7. Optionally saves cleaned data to file
    8. Closes connections
    
    This is what gets called by GitHub Actions / AWS Lambda / Airflow
    """
    logger.info("=" * 60)
    logger.info("STARTING BATCH EVENT PROCESSING JOB")
    logger.info("=" * 60)
    
    # Check for required environment variables
    MONGODB_URI = os.getenv('MONGODB_URI')
    if not MONGODB_URI:
        logger.error("MONGODB_URI environment variable not set")
        raise ValueError("MONGODB_URI environment variable not set")
    
    processor = None
    
    try:
        # Initialize processor
        processor = BatchProcessor()
        
        # Define time range for this batch
        # TEMPORARILY: Process ALL historical data (remove time filters)
        # TODO: Change back to last hour after initial run
        # est = ZoneInfo('America/New_York')
        # end_time = datetime.now(est)
        # start_time = end_time - timedelta(hours=1)
        
        logger.info("Processing ALL historical events from MongoDB")
        
        # Load ALL events (no time filter)
        df = processor.load_events()
        
        if df.empty:
            logger.warning("No events to process in this time range.")
            return
        
        # Clean the data
        cleaned = processor.clean_dataframe(df)
        
        # Engineer features
        featured = processor.add_features(cleaned)
        
        # Update Redis cache (only if Redis is available)
        processor.update_redis_cache(featured)
        
        # ========== ML MODEL TRAINING (COMMENTED OUT FOR NOW) ==========
        # TODO: Implement these functions when ready to add ML capabilities
        # Uncomment this section once you've implemented the ML training functions
        
        # # Train ML models daily at midnight UTC
        # est = ZoneInfo('America/New_York')
        # current_hour = datetime.now(est).hour
        # if current_hour == 0:  # Midnight EST
        #     logger.info("\n" + "=" * 60)
        #     logger.info("STARTING DAILY ML MODEL TRAINING")
        #     logger.info("=" * 60)
        #     
        #     # Load full dataset (last 30 days) for training
        #     training_start = datetime.now(est) - timedelta(days=30)
        #     full_df = processor.load_events(start_date=training_start)
        #     
        #     if len(full_df) > 100:  # Need minimum data
        #         full_cleaned = processor.clean_dataframe(full_df)
        #         full_featured = processor.add_features(full_cleaned)
        #         
        #         # Train user segmentation model (K-means clustering)
        #         # kmeans, scaler, cluster_names = train_user_segmentation_model(full_featured)
        #         # logger.info("✓ User segmentation model trained")
        #         
        #         # Train recommendation engine (collaborative filtering)
        #         # similarity_matrix = train_recommendation_model(full_featured)
        #         # logger.info("✓ Recommendation model trained")
        #         
        #         # Save models to S3 or Redis for dashboard to use
        #         # upload_models_to_storage(kmeans, scaler, similarity_matrix)
        #         
        #         logger.info("=" * 60)
        #         logger.info("ML MODEL TRAINING COMPLETE")
        #         logger.info("=" * 60)
        #     else:
        #         logger.warning("Insufficient data for ML training (need >100 events)")
        
        # ================================================================
        
        # Optional: Save cleaned data to file for analysis
        # Uncomment if you want to save processed data
        # est = ZoneInfo('America/New_York')
        # output_file = f"/tmp/cleaned_events_{datetime.now(est).strftime('%Y%m%d_%H%M%S')}.parquet"
        # featured.to_parquet(output_file)
        # logger.info(f"Saved cleaned data to {output_file}")
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Events processed: {len(featured)}")
        logger.info(f"Unique sessions: {featured['sessionId'].nunique()}")
        logger.info(f"Event types: {featured['eventType'].value_counts().to_dict()}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"✗ Batch processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)  # Exit with error code for GitHub Actions
    
    finally:
        if processor:
            processor.close()
        logger.info("Batch processing job completed")


if __name__ == '__main__':
    """
    This block only runs when the script is executed directly:
        python batch_processor.py
    
    It does NOT run when the script is imported:
        from batch_processor import BatchProcessor
    """
    main()