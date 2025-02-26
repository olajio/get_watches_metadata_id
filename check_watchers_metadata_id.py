from elasticsearch import Elasticsearch


#test_watches_dev_api_key = aXXXXXXXXXXXXXXXXXXXXXXXXWUlUS0tXR1B6YXZWV3FiUQ==
#dev_es_endpoint = https://XXXXXXXXXXXX.us-east-1.aws.found.io


#test_watches_ccs_api_key = WXXXXXXXXXXXXXXXXXXXXXXXXcWxSS2VDbzdqYUZkM2R1dw==
#ccs_es_endpoint = https://XXXXXXXXXXXX.us-east-1.aws.found.io

# Elasticsearch Connection
ELASTICSEARCH_HOST = "https://XXXXXXXXXXXX.us-east-1.aws.found.io"
es = Elasticsearch(ELASTICSEARCH_HOST,
                   api_key="WXXXXXXXXXXXXXXXXXXXXXXXXcWxSS2VDbzdqYUZkM2R1dw==",
                   verify_certs=True)

# File containing watcher IDs
WATCHER_IDS_FILE = "ccs_watcher_ids.txt"


# Read Watcher IDs from file
def load_watcher_ids(file_path):
    with open(file_path, "r") as file:
        return [line.strip() for line in file.readlines() if line.strip()]


# Fetch a single watcher
def get_watcher(watch_id):
    try:
        response = es.get(index=".watches", id=watch_id)
        return response["_source"]
    except Exception as e:
        print(f"Error fetching watcher {watch_id}: {e}")
        return None


# Check watchers and return those with mismatched metadata.id
def check_watchers():
    watcher_ids = load_watcher_ids(WATCHER_IDS_FILE)
    print(f"Loaded {len(watcher_ids)} watcher IDs.")

    mismatched_watcher_ids = []

    for watch_id in watcher_ids:
        watch_data = get_watcher(watch_id)
        if watch_data:
            metadata = watch_data.get("metadata", {})
            metadata_id = metadata.get("id")

            if metadata_id is None or metadata_id != watch_id:
                mismatched_watcher_ids.append(watch_id)

    return mismatched_watcher_ids


# Main function
def main():
    mismatched_watcher_ids = check_watchers()

    if mismatched_watcher_ids:
        print("\nWatchers with non-matching 'metadata.id' vs 'watcher id':")
        for watch_id in mismatched_watcher_ids:
            print(f"    {watch_id}")
    else:
        print("\nAll watchers have correct metadata.id values.")


if __name__ == "__main__":
    main()
