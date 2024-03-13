import logging
import json
import time
from datetime import datetime
from utils import clean_caption, fetch_posts, upsert_post_status, parse_command_line_arguments, load_config, connect_to_database, construct_image_path
from twitter.account import Account


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def login(credentials):
    account = Account(credentials['email'], credentials['username'], credentials['password'])
    return account

def post_to_twitter(account, post, image_directory):
    tweet_content = clean_caption(post['content'])
    total_images = len(post['original_pictures'].split(',')) if post['original_pictures'] else 0
    max_images_allowed = 4
    image_paths = [
        f"{image_directory}/{construct_image_path(post['publish_time'], post['id'], i+1)}"
        for i in range(min(total_images, max_images_allowed))
    ]

    media = []
    # Prepare media for the tweet
    for image_path in image_paths:
        media.append({
            'media': image_path,  # Assuming this function expects a file path
            'alt': 'Image description',  # Optionally provide alt text for each image
            'tagged_users': []  # Optionally tag users in each image
        })
    try:
        # Post the tweet with the media
        response = account.tweet(text=tweet_content, media=media)
        return 'completed'
    except Exception as e:
        logging.error(f"Error posting {post['id']} to Twitter: {e}")
        return 'failed'

def main():
    args = parse_command_line_arguments() 
    batch_number = args.batch
    config = load_config()
    db_connection = connect_to_database(config['database'])
    batch_size = config.get('batch_size', 20)  # Default to 20 if not specified
    try:
        posts = fetch_posts(db_connection, "twitter", batch_number, batch_size)
        account = login(config['twitter'])
        
        for post in posts:
            status = post_to_twitter(account, post, config['local_image_directory'])
            logging.info(f"Posted to Twitter: {post['id']}")
            upsert_post_status(db_connection, post['id'], 'twitter', status)
            time.sleep(config.get('post_delay_seconds', 5))  # Default to 5 seconds if not specified
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db_connection.close()

if __name__ == "__main__":
    main()