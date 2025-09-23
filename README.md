# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project analyzes user listening behavior using PySpark. It loads raw listening logs and song metadata, derives insights about listening preferences, and writes the results to structured output folders for downstream consumption or reporting.

## Dataset Description
- `listening_logs.csv`: Contains `user_id`, `song_id`, `timestamp`, and `duration_sec` columns representing each play event.
- `songs_metadata.csv`: Contains `song_id`, `title`, `artist`, `genre`, and `mood` columns describing each track referenced in the logs.

## Approach
1. **Load and join data**: Both CSVs are read with headers and inferred schemas. Listening events are enriched with song genres via a join on `song_id`.
2. **User favorite genre**: Plays are aggregated by `(user_id, genre)` and a window function selects the genre with the highest play count per user (ties broken alphabetically). Results are saved to `output/user_favorite_genres/`.
3. **Average listen time per song**: Average `duration_sec` is computed per `song_id` and combined with song title and artist metadata. Outputs are written to `output/avg_listen_time_per_song/`.
4. **Genre loyalty score**: For each user, the share of plays coming from their favorite genre is calculated. Users with a loyalty score greater than `0.8` are exported to `output/genre_loyalty_scores/`.
5. **Night owl users**: Timestamps are converted to Spark timestamps to extract the hour of day. Users with plays logged between 00:00 and 04:59 are summarized with counts and ratios in `output/night_owl_users/`.

## Results
- **Favorite genres**: Each row reports a user’s top genre and how many plays contributed to that ranking.
- **Average listen time**: Provides per-song averages that can surface tracks typically skipped early or played longer.
- **Genre loyalty**: Highlights highly focused listeners whose plays largely remain within a single genre.
- **Night owl users**: Identifies users active overnight, along with how significant late-night listening is relative to their total activity.

## Repository Structure
- `main.py`: Spark job implementing all analysis tasks.
- `datagen.py`: Script to regenerate sample input CSVs.
- `listening_logs.csv`, `songs_metadata.csv`: Sample datasets used by the Spark job.
- `output/`: Folder created by the job containing subdirectories for each task result.

## Execution Instructions
### Prerequisites
Ensure the following software is installed and accessible in your PATH:

1. **Python 3.x**
   ```bash
   python3 --version
   ```
2. **PySpark**
   ```bash
   pip install pyspark
   ```
3. **Apache Spark**
   ```bash
   spark-submit --version
   ```

### Running the Analysis
1. *(Optional)* Regenerate inputs if needed:
   ```bash
   python3 datagen.py
   ```
2. Execute the Spark job:
   ```bash
   spark-submit main.py
   ```
3. Inspect results:
   ```bash
   tree output/
   ```

## Troubleshooting
- If `spark-submit` is not found, verify your Spark installation and environment variables.
- When running on large datasets, adjust Spark configuration (executor memory, shuffle partitions) as appropriate.
