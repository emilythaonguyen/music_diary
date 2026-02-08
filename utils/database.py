"""
Database helper utilities for common operations.
"""
import asyncpg
from typing import Optional, Any, List, Tuple, Iterator
from datetime import datetime


class DatabaseHelper:
    """Helper class for common database operations."""
    
    @staticmethod
    async def get_or_create(
        pool: asyncpg.Pool,
        table: str,
        pk_column: str,
        unique_column: str,
        unique_value: Any,
        insert_data: dict,
        conflict_column: Optional[str] = None
    ) -> int:
        """
        Generic get-or-create pattern for database entities.
        
        Args:
            pool: Database connection pool
            table: Table name
            pk_column: Primary key column name
            unique_column: Column to check for existing record
            unique_value: Value to check
            insert_data: Dictionary of column:value pairs for INSERT
            conflict_column: Column for ON CONFLICT (defaults to unique_column)
        
        Returns:
            Primary key of existing or newly created record
        """
        conflict_column = conflict_column or unique_column
        
        async with pool.acquire() as conn:
            # try to find existing
            if unique_value:
                existing_id = await conn.fetchval(
                    f"SELECT {pk_column} FROM {table} WHERE {unique_column} = $1",
                    unique_value
                )
                if existing_id:
                    return existing_id
            
            # build insert statement
            columns = list(insert_data.keys())
            placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
            values = [insert_data[col] for col in columns]
            
            # handle conflict
            if conflict_column and conflict_column in columns:
                update_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col != pk_column and col != conflict_column
                )
                
                query = f"""
                    INSERT INTO {table} ({", ".join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_column}) DO UPDATE 
                        SET {update_clause}
                    RETURNING {pk_column}
                """
            else:
                query = f"""
                    INSERT INTO {table} ({", ".join(columns)})
                    VALUES ({placeholders})
                    RETURNING {pk_column}
                """
            
            try:
                return await conn.fetchval(query, *values)
            except asyncpg.IntegrityError:
                # race condition - try to fetch again
                if unique_value:
                    existing_id = await conn.fetchval(
                        f"SELECT {pk_column} FROM {table} WHERE {unique_column} = $1",
                        unique_value
                    )
                    if existing_id:
                        return existing_id
                raise


async def check_metadata_refresh(
    pool: asyncpg.Pool,
    table: str,
    id_column: str,
    entity_id: int,
    threshold_days: int = 30
) -> bool:
    """
    Check if metadata needs refreshing based on retrieved_at timestamp.
    
    Args:
        pool: Database connection pool
        table: Table name
        id_column: ID column name
        entity_id: Entity ID
        threshold_days: Days before refresh needed
    
    Returns:
        True if refresh needed, False otherwise
    """
    from datetime import timezone, timedelta
    
    query = f"SELECT retrieved_at FROM {table} WHERE {id_column} = $1"
    retrieved_at = await pool.fetchval(query, entity_id)
    
    if not retrieved_at:
        return True
    
    # make timezone-aware if needed
    if retrieved_at.tzinfo is None:
        from datetime import timezone
        retrieved_at = retrieved_at.replace(tzinfo=timezone.utc)
    
    age = datetime.now(timezone.utc) - retrieved_at
    return age > timedelta(days=threshold_days)


def parse_mb_date(date_val: Optional[str]) -> Optional[Any]:
    """
    Parse MusicBrainz date string to datetime.date.
    Handles 'YYYY', 'YYYY-MM', 'YYYY-MM-DD' formats.
    
    Args:
        date_val: Date string from MusicBrainz
    
    Returns:
        datetime.date object or None
    """
    if not isinstance(date_val, str):
        return None
    
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(date_val, fmt).date()
        except ValueError:
            continue
    
    return None


def chunked(iterable: List[Any], size: int) -> Iterator[List[Any]]:
    """
    Split an iterable into chunks of specified size.
    
    Args:
        iterable: List to chunk
        size: Chunk size
    
    Yields:
        Chunks of the original list
    """
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


async def batch_execute(
    pool: asyncpg.Pool,
    query: str,
    data: List[Tuple],
    batch_size: int = 100
) -> None:
    """
    Execute a query in batches for better performance.
    
    Args:
        pool: Database connection pool
        query: SQL query with placeholders
        data: List of tuples containing query parameters
        batch_size: Number of records per batch
    """
    async with pool.acquire() as conn:
        for batch in chunked(data, batch_size):
            await conn.executemany(query, batch)


class ProgressTracker:
    """Track progress of long-running operations."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
    
    def load(self) -> Optional[int]:
        """Load the last processed timestamp/ID from file."""
        try:
            with open(self.filepath) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None
    
    def save(self, value: int) -> None:
        """Save the latest processed timestamp/ID to file."""
        with open(self.filepath, "w") as f:
            f.write(str(value))