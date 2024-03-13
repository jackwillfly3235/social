import logging
import time
from instagrapi import Client
from datetime import datetime
from utils import clean_caption, fetch_posts, upsert_post_status, parse_command_line_arguments, load_config, connect_to_database, construct_image_path


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def login(credentials):
    client = Client()
    client.login(credentials['username'], credentials['password'])
    return client

def post_to_instagram(client, post, image_directory):
    captions = clean_caption(post['content'])
    total_images = len(post['original_pictures'].split(',')) if post['original_pictures'] else 0
    image_paths = [f"{image_directory}/{construct_image_path(post['publish_time'], post['id'], i+1)}" for i in range(total_images)]
    try:
        if total_images > 0:
            # For multiple images, use album_upload
            if total_images > 1:
                client.album_upload(image_paths, caption=captions)
            # For a single image, use photo_upload
            else:
                client.photo_upload(image_paths[0], caption=captions)
            return ('completed', None)  # Return status and no error
        else:
            logging.warning(f"No images to post for {post['id']}.")
            return ('failed', 'No images to post') 
    except Exception as e:
        logging.error(f"Error posting {post['id']}: {e}")
        return ('failed', str(e))

def main():
    args = parse_command_line_arguments() 
    batch_number = args.batch
    config = load_config()
    db_connection = connect_to_database(config['database'])
    batch_size = config.get('batch_size', 20)  # Default to 20 if not specified
    try:
        posts = fetch_posts(db_connection, "instagram", batch_number, batch_size)
        client = login(config['instagram'])
        
        for post in posts:
            status, error_message = post_to_instagram(client, post, config['local_image_directory'])
            logging.info(f"Posted to Instagram: {post['id']}")
            upsert_post_status(db_connection, post['id'], 'instagram', status, error_message)
            time.sleep(config.get('post_delay_seconds', 5))  # Default to 5 seconds if not specified
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db_connection.close()

if __name__ == "__main__":
    main()
