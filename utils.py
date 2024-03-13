import mysql.connector
import re
import argparse
import logging
import json

def upsert_post_status(db_connection, weibo_id, platform, status, error_message=None):
    query = """
    INSERT INTO posts (weibo_id, social_platform, status, errors) 
    VALUES (%s, %s, %s, %s) 
    ON DUPLICATE KEY UPDATE 
    status = VALUES(status), errors = VALUES(errors), timestamp = CURRENT_TIMESTAMP
    """
    with db_connection.cursor() as cursor:
        # Ensure error_message is explicitly set to NULL if it's None, as some DBs might require this
        error_message = error_message or None
        cursor.execute(query, (weibo_id, platform, status, error_message))
        db_connection.commit()

def fetch_posts(db_connection, social_platform, batch_number=1, batch_size=20):
    offset = (batch_number - 1) * batch_size
    query = """
    SELECT w.* FROM weibo w
    LEFT JOIN posts p ON w.id = p.weibo_id AND p.social_platform = %s
    WHERE p.status IS NULL OR p.status <> 'completed'
    ORDER BY w.publish_time ASC
    LIMIT %s OFFSET %s
    """
    logging.info(f"Executing query: {query.strip()} with parameters: social_platform={social_platform}, batch_size={batch_size}, offset={offset}")

    with db_connection.cursor(dictionary=True) as cursor:
        cursor.execute(query, (social_platform, batch_size, offset))
        return cursor.fetchall()
    
def clean_caption(caption):
    # Pattern to find "[组图共X张] 原图" where X is any number of digits
    pattern = r"\[组图共\d+张\]\s*原图\s*"
    # Replace the found pattern with an empty string to remove it
    cleaned_caption = re.sub(pattern, "", caption)
    return cleaned_caption

def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='Post images and captions to Instagram from a MySQL database.')
    parser.add_argument('--batch', type=int, default=1, help='Batch number for fetching records')
    args = parser.parse_args()
    return args

# Database connection configuration
def load_config(path='config.json'):
    with open(path) as config_file:
        return json.load(config_file)

def connect_to_database(config):
    return mysql.connector.connect(**config)

def construct_image_path(publish_time, post_id, order_number=1):
    date_str = publish_time.strftime('%Y%m%d')
    return f"{date_str}_{post_id}_{order_number}.jpg"