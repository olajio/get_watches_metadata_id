from elasticsearch import Elasticsearch

# Elasticsearch Connection
ELASTICSEARCH_HOST = "http://localhost:9200"  # Modify if needed
es = Elasticsearch(ELASTICSEARCH_HOST)

# File containing watcher IDs
WATCHER_IDS_FILE = "watcher_ids.txt"

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

    mismatched_watchers = []

    for watch_id in watcher_ids:
        watch_data = get_watcher(watch_id)
        if watch_data:
            metadata = watch_data.get("metadata", {})
            metadata_id = metadata.get("id")

            if metadata_id is None or metadata_id != watch_id:
                mismatched_watchers.append(watch_id)

    return mismatched_watchers

# Main function
def main():
    mismatched_watchers = check_watchers()
    
    if mismatched_watchers:
        print("\nWatchers with incorrect metadata.id:")
        for watch_id in mismatched_watchers:
            print(watch_id)
    else:
        print("\nAll watchers have correct metadata.id values.")

if __name__ == "__main__":
    main()
